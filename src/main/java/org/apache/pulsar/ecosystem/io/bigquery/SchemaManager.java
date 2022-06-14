/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.pulsar.ecosystem.io.bigquery;

import static com.google.common.base.Preconditions.checkState;
import static java.net.HttpURLConnection.HTTP_NOT_FOUND;
import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.BigQueryException;
import com.google.cloud.bigquery.Clustering;
import com.google.cloud.bigquery.Field;
import com.google.cloud.bigquery.FieldList;
import com.google.cloud.bigquery.LegacySQLTypeName;
import com.google.cloud.bigquery.Schema;
import com.google.cloud.bigquery.StandardTableDefinition;
import com.google.cloud.bigquery.Table;
import com.google.cloud.bigquery.TableId;
import com.google.cloud.bigquery.TableInfo;
import com.google.cloud.bigquery.TimePartitioning;
import com.google.cloud.bigquery.storage.v1.BQTableSchemaToProtoDescriptor;
import com.google.cloud.bigquery.storage.v1.ProtoSchema;
import com.google.cloud.bigquery.storage.v1.ProtoSchemaConverter;
import com.google.cloud.bigquery.storage.v1.TableFieldSchema;
import com.google.cloud.bigquery.storage.v1.TableName;
import com.google.cloud.bigquery.storage.v1.TableSchema;
import com.google.protobuf.Descriptors;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import lombok.Getter;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.client.api.schema.GenericObject;
import org.apache.pulsar.ecosystem.io.bigquery.convert.schema.SchemaConvert;
import org.apache.pulsar.ecosystem.io.bigquery.convert.schema.SchemaConvertHandler;
import org.apache.pulsar.ecosystem.io.bigquery.exception.BigQueryConnectorRuntimeException;
import org.apache.pulsar.functions.api.Record;

/**
 * Schema manager.
 */
@Slf4j
public class SchemaManager {

    private final BigQuery bigquery;
    private final TableId tableId;
    private final TableName tableName;
    @Getter
    private Schema schema;
    @Getter
    private TableSchema tableSchema;
    @Getter
    private Descriptors.Descriptor descriptor;
    @Getter
    private ProtoSchema protoSchema;

    private final SchemaConvert schemaConvert;

    // config info
    private final Set<String> defaultSystemField;
    private final boolean autoCreateTable;
    private final boolean autoUpdateTable;
    private final boolean partitionedTables;
    private final int partitionedTableIntervalDay;
    private final boolean clusteredTables;

    public SchemaManager(BigQueryConfig bigQueryConfig) throws IOException {
        this.bigquery = bigQueryConfig.createBigQuery();
        this.tableId = bigQueryConfig.getTableId();
        this.tableName = bigQueryConfig.getTableName();
        this.defaultSystemField = bigQueryConfig.getDefaultSystemField();
        this.autoCreateTable = bigQueryConfig.isAutoCreateTable();
        this.autoUpdateTable = bigQueryConfig.isAutoUpdateTable();
        this.partitionedTables = bigQueryConfig.isPartitionedTables();
        this.partitionedTableIntervalDay = bigQueryConfig.getPartitionedTableIntervalDay();
        this.clusteredTables = bigQueryConfig.isClusteredTables();
        this.schemaConvert = new SchemaConvertHandler(defaultSystemField);
    }

    /**
     * Create table when table doesn't exist.
     * Each call will perform a request to try to get the latest schema.
     *
     * @param records
     */
    public Schema initTable(Record<GenericObject> records) {
        try {
            Schema schema = fetchTableSchema();
            updateCacheSchema(schema);
            log.info("Table is exist <{}>, ignore create request.", tableId);
            return schema;
        } catch (BigQueryException e) {
            if (e.getCode() == HTTP_NOT_FOUND) {
                if (autoCreateTable) {
                    log.info("Table is not exist and auto create table equals true, start creating table.");
                } else {
                    throw new BigQueryConnectorRuntimeException(
                            "Init table failed, table is not exist and autoCreateTable == false");
                }
            } else {
                throw e;
            }
        }

       Schema schema = schemaConvert.convertSchema(records);
        TableInfo tableInfo = TableInfo.newBuilder(tableId, getTableInfo(schema)).build();
       bigquery.create(tableInfo);
       updateCacheSchema(schema);
       log.info("create table success <{}>", tableId);
        return schema;
    }

    private StandardTableDefinition getTableInfo(Schema schema) {
        StandardTableDefinition.Builder tableDefinition = StandardTableDefinition.newBuilder()
                .setSchema(schema);
        if (partitionedTables) {
            TimePartitioning partitioning =
                    TimePartitioning.newBuilder(TimePartitioning.Type.DAY)
                            .setField("__event_time__") //  name of column to use for partitioning
                            .setExpirationMs(TimeUnit.MILLISECONDS.
                                    convert(partitionedTableIntervalDay, TimeUnit.DAYS))
                            .build();
            tableDefinition.setTimePartitioning(partitioning);
        }
        if (clusteredTables) {
            Clustering clustering =
                    Clustering.newBuilder().setFields(Collections.singletonList("__message_id__")).build();
            tableDefinition.setClustering(clustering);
        }
        return tableDefinition.build();
    }

    /**
     * Update schema, update role:
     * 1. Merge pulsar schema and bigquery schema fields.
     * 2. Set the Mode of the newly added field to NULLABLE and cancel the required option.
     * 3. Even if the field does not exist in pulsar schema, it will not be deleted in big query.
     *
     * @param records
     */
    public void updateSchema(List<Record<GenericObject>> records) {
        if (!autoUpdateTable) {
            throw new BigQueryConnectorRuntimeException("Table cannot be update,"
                    + " autoUpdateTable == false.");
        }
        Schema bigQuerySchema = initTable(records.get(0));
        List<Schema> schemas =
                records.stream().map(record -> schemaConvert.convertSchema(record)).collect(Collectors.toList());
        Schema mergeSchema = bigQuerySchema;
        for (Schema toMergeSchema : schemas) {
            mergeSchema = mergeSchema(toMergeSchema, mergeSchema);
        }
        TableInfo tableInfo = Table.newBuilder(tableId, getTableInfo(mergeSchema)).build();
        bigquery.update(tableInfo);
        updateCacheSchema(mergeSchema);
        log.info("update table success <{}>", tableInfo);
    }

    public void updateSchema(Record<GenericObject> record) {
        updateSchema(Arrays.asList(record));
    }

    // ---------------------------- private method -------------------------------
    private List<TableFieldSchema> toTableFieldsSchema(FieldList fieldsList) {
        List<TableFieldSchema> tableFieldSchemas = new ArrayList<>(fieldsList.size());
        for (Field field : fieldsList) {
            TableFieldSchema.Builder tableField = TableFieldSchema.newBuilder().setName(field.getName());
            if (field.getType() == LegacySQLTypeName.FLOAT) {
                tableField.setType(TableFieldSchema.Type.DOUBLE);
            } else {
                tableField.setType(TableFieldSchema.Type.valueOf(field.getType().getStandardType().name()));
            }
            tableField.setMode(TableFieldSchema.Mode.valueOf(field.getMode().name()));
            Optional.ofNullable(field.getDescription()).ifPresent(
                    description -> tableField.setDescription(description));
            if (field.getType() == LegacySQLTypeName.RECORD) {
                FieldList subFields = field.getSubFields();
                for (TableFieldSchema tableFieldSchema : toTableFieldsSchema(subFields)) {
                    tableField.addFields(tableFieldSchema);
                }
            }
            tableFieldSchemas.add(tableField.build());
        }
        return tableFieldSchemas;
    }

    private Schema mergeSchema(Schema bigQuerySchema, Schema pulsarSchema) {
        Map<String, Field> bigQueryFields = getSchemaFields(bigQuerySchema);
        Map<String, Field> pulsarFields = getSchemaFields(pulsarSchema);
        Map<String, Field> mergeFields = new LinkedHashMap<>();
        pulsarFields.forEach((name, pulsarField) -> {
            Field bigqueryField = bigQueryFields.get(name);
            mergeFields.put(name, mergeFields(bigqueryField, pulsarField));
        });
        // If missing fields by pulsar schema, set these field type to NULLABLE
        addMergeFields(bigQueryFields, mergeFields);
        return Schema.of(mergeFields.values());
    }

    private Field mergeFields(Field bigQueryField, Field pulsarField) {

        if (bigQueryField == null && pulsarField == null) {
            throw new IllegalArgumentException("Both fields cannot be null at the same time");
        }

        if (bigQueryField == null || pulsarField == null) {
            Field sourceField = bigQueryField == null ? pulsarField : bigQueryField;
            // add field must is REQUIRED or NULLABLE
            if (!Field.Mode.REPEATED.equals(sourceField.getMode())) {
                return sourceField.toBuilder().setMode(Field.Mode.NULLABLE).build();
            } else {
                return sourceField;
            }
        }

        checkState(bigQueryField.getName().equals(pulsarField.getName()),
                String.format("Field name different, bigQueryFieldName: %s, pulsarFieldName: %s",
                        bigQueryField.getName(), pulsarField.getName()));
        checkState(bigQueryField.getType() == pulsarField.getType(),
                String.format("Field type different, bigQueryFieldType: %s, pulsarFieldType: %s",
                        bigQueryField.getType(), pulsarField.getType()));

        Field.Builder fieldBuild = pulsarField.toBuilder();
        if (pulsarField.getType() == LegacySQLTypeName.RECORD) {
            Map<String, Field> bqSubFields = getSubFields(bigQueryField);
            Map<String, Field> pulsarSubFields = getSubFields(pulsarField);
            Map<String, Field> mergeSubFields = new LinkedHashMap<>();
            pulsarSubFields.forEach((name, pulsarSubField) -> {
                Field bqSubField = bqSubFields.get(name);
                mergeSubFields.put(name, mergeFields(bqSubField, pulsarSubField));
            });
            // If missing fields by pulsar schema, set these field type to NULLABLE
            addMergeFields(bqSubFields, mergeSubFields);
            fieldBuild.setType(LegacySQLTypeName.RECORD, mergeSubFields.values().toArray(new Field[]{}));
        }
        return fieldBuild.build();
    }

    private void addMergeFields(Map<String, Field> bigQueryFields, Map<String, Field> mergeFields) {
        bigQueryFields.forEach((name, bigqueryField) -> {
            if (!mergeFields.containsKey(name)) {
                if (!Field.Mode.REPEATED.equals(bigqueryField.getMode())) {
                    mergeFields.put(name, bigqueryField.toBuilder().setMode(Field.Mode.NULLABLE).build());
                } else {
                    mergeFields.put(name, bigqueryField);
                }
            }
        });
    }

    private Map<String, Field> getSubFields(Field parent) {
        Map<String, Field> result = new LinkedHashMap<>();
        if (parent == null || parent.getSubFields() == null) {
            return result;
        }
        parent.getSubFields().forEach(field -> {
            if (field.getMode() == null) {
                field = field.toBuilder().setMode(Field.Mode.NULLABLE).build();
            }
            result.put(field.getName(), field);
        });
        return result;
    }

    private Map<String, Field> getSchemaFields(Schema schema) {
        Map<String, Field> result = new LinkedHashMap<>();
        schema.getFields().forEach(field -> result.put(field.getName(), field));
        return result;
    }

    @SneakyThrows
    private void updateCacheSchema(Schema schema) {
        if (schema == null) {
            this.schema = null;
            this.tableSchema = null;
            this.descriptor = null;
            this.protoSchema = null;
        } else {
            this.schema = schema;
            this.tableSchema = TableSchema.newBuilder().addAllFields(toTableFieldsSchema(schema.getFields())).build();
            this.descriptor = BQTableSchemaToProtoDescriptor.convertBQTableSchemaToProtoDescriptor(this.tableSchema);
            this.protoSchema = ProtoSchemaConverter.convert(this.descriptor);
        }
    }

    private Schema fetchTableSchema() {
        return Optional.ofNullable(bigquery.getTable(tableId))
                .orElseThrow(() -> new BigQueryException(HTTP_NOT_FOUND, "Not Found table:" + tableId))
                .getDefinition().getSchema();
    }
}
