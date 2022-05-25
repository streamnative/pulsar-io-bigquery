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
import com.google.cloud.bigquery.LegacySQLTypeName;
import com.google.cloud.bigquery.Schema;
import com.google.cloud.bigquery.StandardTableDefinition;
import com.google.cloud.bigquery.Table;
import com.google.cloud.bigquery.TableId;
import com.google.cloud.bigquery.TableInfo;
import com.google.cloud.bigquery.TimePartitioning;
import java.io.IOException;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.client.api.schema.GenericRecord;
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
    private final SchemaConvert schemaConvert;

    // config info
    private final Set<String> defaultSystemField;
    private final boolean autoCreateTable;
    private final boolean autoUpdateTable;
    private final boolean partitionedTables;
    private final int partitionedTableIntervalDay;
    private final boolean clusteredTables;

    // schema resources
    private Schema currentSchema;

    public SchemaManager(BigQueryConfig bigQueryConfig) throws IOException {
        this.bigquery = bigQueryConfig.createBigQuery();
        this.tableId = TableId.of(
                bigQueryConfig.getProjectId(), bigQueryConfig.getDatasetName(), bigQueryConfig.getTableName());
        this.defaultSystemField = bigQueryConfig.getDefaultSystemField();
        this.autoCreateTable = bigQueryConfig.isAutoCreateTable();
        this.autoUpdateTable = bigQueryConfig.isAutoUpdateTable();
        this.partitionedTables = bigQueryConfig.isPartitionedTables();
        this.partitionedTableIntervalDay = bigQueryConfig.getPartitionedTableIntervalDay();
        this.clusteredTables = bigQueryConfig.isClusteredTables();
        this.schemaConvert = new SchemaConvertHandler(defaultSystemField);

        // init current schema
        try {
            currentSchema = fetchTableSchema();
        } catch (BigQueryException e) {
            if (e.getCode() == HTTP_NOT_FOUND) {
                if (!autoCreateTable) {
                    log.error("Not found table {} and auto create table is disable", tableId, e);
                    throw e;
                } else {
                    log.info("Not found table {}, when first message received to auto create", tableId);
                }
            } else {
                throw e;
            }
        }
    }

    /**
     * Create table when table doesn't exist.
     *
     * @param records
     */
    public void createTable(Record<GenericRecord> records) {
        // There is no need to judge whether autoCreateTable is open here,
        // the scene that is not opened has been intercepted in the constructor.
        if (currentSchema != null) {
            return;
        }
        try {
            Schema schema = schemaConvert.convertSchema(records);
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
                        Clustering.newBuilder().setFields(Arrays.asList("__message_id__")).build();
                tableDefinition.setClustering(clustering);
            }
            TableInfo tableInfo = TableInfo.newBuilder(tableId, tableDefinition.build()).build();
            bigquery.create(tableInfo);
            currentSchema = schema;
        } catch (Exception e) {
            throw e;
        }
    }

    /**
     * Update schema, update role:
     * 1. Merge pulsar schema and bigquery schema fields.
     * 2. Set the Mode of the newly added field to NULLABLE and cancel the required option.
     * 3. Even if the field does not exist in pulsar schema, it will not be deleted in big query.
     *
     * @param records
     */
    public void updateSchema(Record<GenericRecord> records) {
        if (!autoUpdateTable) {
            throw new BigQueryConnectorRuntimeException("Table cannot be update,"
                    + " autoUpdateTable == false.");
        }
        Schema bigQuerySchema = fetchTableSchema();
        Schema pulsarSchema = schemaConvert.convertSchema(records);
        Schema mergeSchema = mergeSchema(bigQuerySchema, pulsarSchema);
        TableInfo tableInfo = Table.newBuilder(tableId, StandardTableDefinition.of(mergeSchema)).build();
        bigquery.update(tableInfo);
        log.info("update table success {}", tableInfo);
    }

    // ---------------------------- private method -------------------------------

    private Schema mergeSchema(Schema bigQuerySchema, Schema pulsarSchema) {
        Map<String, Field> bigQueryFields = getSchemaFields(bigQuerySchema);
        Map<String, Field> pulsarFields = getSchemaFields(pulsarSchema);
        Map<String, Field> mergeFields = new LinkedHashMap<>();
        pulsarFields.forEach((name, pulsarField) -> {
            Field bigqueryField = bigQueryFields.get(name);
            if (bigqueryField == null) {
                // add field must is REQUIRED or NULLABLE
                if (!Field.Mode.REPEATED.equals(pulsarField.getMode())) {
                    mergeFields.put(name, pulsarField.toBuilder().setMode(Field.Mode.NULLABLE).build());
                } else {
                    mergeFields.put(name, pulsarField);
                }
            } else {
                mergeFields.put(name, mergeFields(bigqueryField, pulsarField));
            }
        });
        // If missing fields by pulsar schema, set these field type to NULLABLE
        addMergeFields(bigQueryFields, mergeFields);
        return Schema.of(mergeFields.values());
    }

    private Field mergeFields(Field bigQueryField, Field pulsarField) {
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
                if (bqSubField == null) {
                    // add field must is REPEATED or NULLABLE
                    if (!Field.Mode.REQUIRED.equals(pulsarSubField.getMode())) {
                        mergeSubFields.put(name, pulsarSubField.toBuilder().setMode(Field.Mode.NULLABLE).build());
                    } else {
                        mergeSubFields.put(name, pulsarSubField);
                    }
                } else {
                    mergeSubFields.put(name, mergeFields(bqSubField, pulsarSubField));
                }
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

    private Schema fetchTableSchema() {
        currentSchema = Optional.ofNullable(bigquery.getTable(tableId))
                .map(t -> t.getDefinition().getSchema())
                .orElse(null);
        return currentSchema;
    }
}
