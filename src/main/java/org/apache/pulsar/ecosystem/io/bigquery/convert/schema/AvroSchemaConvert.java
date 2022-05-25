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
package org.apache.pulsar.ecosystem.io.bigquery.convert.schema;

import static org.apache.pulsar.ecosystem.io.bigquery.convert.DefaultSystemFieldConvert.MAP_KEY_NAME;
import static org.apache.pulsar.ecosystem.io.bigquery.convert.DefaultSystemFieldConvert.MAP_VALUE_NAME;
import com.google.cloud.bigquery.Field;
import com.google.cloud.bigquery.FieldList;
import com.google.cloud.bigquery.StandardSQLTypeName;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import org.apache.pulsar.client.api.schema.GenericRecord;
import org.apache.pulsar.ecosystem.io.bigquery.convert.logicaltype.AvroLogicalFieldConvert;
import org.apache.pulsar.ecosystem.io.bigquery.exception.BigQueryConnectorRuntimeException;
import org.apache.pulsar.functions.api.Record;
import org.apache.pulsar.shade.org.apache.avro.LogicalTypes;


/**
 * Avro schema convert.
 */
public class AvroSchemaConvert extends AbstractSchemaConvert {

    private static final Map<org.apache.pulsar.shade.org.apache.avro.Schema.Type, StandardSQLTypeName>
            PRIMITIVE_TYPE_MAP = new HashMap<>();

    private AvroLogicalFieldConvert logicalFieldConvert;

    static {
        PRIMITIVE_TYPE_MAP.put(org.apache.pulsar.shade.org.apache.avro.Schema.Type.ENUM, StandardSQLTypeName.STRING);
        PRIMITIVE_TYPE_MAP.put(org.apache.pulsar.shade.org.apache.avro.Schema.Type.FIXED, StandardSQLTypeName.BYTES);
        PRIMITIVE_TYPE_MAP.put(org.apache.pulsar.shade.org.apache.avro.Schema.Type.STRING, StandardSQLTypeName.STRING);
        PRIMITIVE_TYPE_MAP.put(org.apache.pulsar.shade.org.apache.avro.Schema.Type.BYTES, StandardSQLTypeName.BYTES);
        PRIMITIVE_TYPE_MAP.put(org.apache.pulsar.shade.org.apache.avro.Schema.Type.INT, StandardSQLTypeName.INT64);
        PRIMITIVE_TYPE_MAP.put(org.apache.pulsar.shade.org.apache.avro.Schema.Type.LONG, StandardSQLTypeName.INT64);
        PRIMITIVE_TYPE_MAP.put(org.apache.pulsar.shade.org.apache.avro.Schema.Type.FLOAT, StandardSQLTypeName.FLOAT64);
        PRIMITIVE_TYPE_MAP.put(org.apache.pulsar.shade.org.apache.avro.Schema.Type.DOUBLE, StandardSQLTypeName.FLOAT64);
        PRIMITIVE_TYPE_MAP.put(org.apache.pulsar.shade.org.apache.avro.Schema.Type.BOOLEAN, StandardSQLTypeName.BOOL);
    }

    public AvroSchemaConvert(Set<String> systemFieldNames) {
        super(systemFieldNames);
        logicalFieldConvert = new AvroLogicalFieldConvert();
    }

    @Override
    protected List<Field> convertUserSchema(Record<GenericRecord> record) {
        org.apache.pulsar.shade.org.apache.avro.generic.GenericRecord nativeRecord =
                (org.apache.pulsar.shade.org.apache.avro.generic.GenericRecord) record.getValue().getNativeObject();
        org.apache.pulsar.shade.org.apache.avro.Schema schema = nativeRecord.getSchema();
        List<Field> userField = new ArrayList<>();
        for (org.apache.pulsar.shade.org.apache.avro.Schema.Field field : schema.getFields()) {
            userField.add(convertField(field.name(), field.schema()).get().build());
        }
        return userField;
    }

    private Optional<Field.Builder> convertField(String fieldName,
                                                 org.apache.pulsar.shade.org.apache.avro.Schema schema) {
        Optional<Field.Builder> result;
        // Union types require special handling refer to the usage documentation.
        // Reference avro docs: https://avro.apache.org/docs/current/spec.html#Unions
        if (schema.isUnion()) {
            schema = schema.getTypes().get(1);
        }
        if (schema.getLogicalType() != null) {
            StandardSQLTypeName standardSQLTypeName = logicalFieldConvert.convertFieldType(schema.getLogicalType());
            Field.Builder fieldBuilder = Field.newBuilder(fieldName, standardSQLTypeName).setMode(Field.Mode.NULLABLE);
            if (standardSQLTypeName == StandardSQLTypeName.BIGNUMERIC
                    || standardSQLTypeName == StandardSQLTypeName.NUMERIC) {
                LogicalTypes.Decimal logicalType = (LogicalTypes.Decimal) schema.getLogicalType();
                fieldBuilder.setPrecision((long) logicalType.getPrecision());
                fieldBuilder.setScale((long) logicalType.getScale());
            }
            result = Optional.of(fieldBuilder);
        } else if (PRIMITIVE_TYPE_MAP.containsKey(schema.getType())) {
            result = Optional.of(Field.newBuilder(fieldName,
                    PRIMITIVE_TYPE_MAP.get(schema.getType())).setMode(Field.Mode.NULLABLE));
        } else {
            switch (schema.getType()) {
                case RECORD:
                    result = Optional.of(convertRecord(fieldName, schema));
                    break;
                case ARRAY:
                    result = Optional.of(convertArray(fieldName, schema));
                    break;
                case MAP:
                    // convertMap logical is hit only if the type of key is String
                    result = Optional.of(convertMap(fieldName, schema));
                    break;
                case NULL:
                    result = Optional.ofNullable(null);
                    break;
                default:
                    throw new BigQueryConnectorRuntimeException(
                            "AVRO schema not support field type:" + schema.getType());
            }
        }
        org.apache.pulsar.shade.org.apache.avro.Schema finalSchema = schema;
        return result.map(res -> {
            if (finalSchema.getDoc() != null) {
                res.setDescription(finalSchema.getDoc());
            }
            return res;
        });
    }

    private Field.Builder convertRecord(String fieldName,
                                        org.apache.pulsar.shade.org.apache.avro.Schema avroFieldSchema) {
        List<Field> fields = new ArrayList<>();
        for (org.apache.pulsar.shade.org.apache.avro.Schema.Field avroField : avroFieldSchema.getFields()) {
            convertField(avroField.name(), avroField.schema()).ifPresent(fieldBuild -> fields.add(fieldBuild.build()));
        }
        return Field
                .newBuilder(fieldName, StandardSQLTypeName.STRUCT, FieldList.of(fields))
                .setMode(Field.Mode.NULLABLE);
    }

    private Field.Builder convertArray(String fieldName,
                                       org.apache.pulsar.shade.org.apache.avro.Schema avroFieldSchema) {
        Optional<Field.Builder> builder = convertField(fieldName, avroFieldSchema.getElementType());
        return builder.get().setMode(Field.Mode.REPEATED);
    }

    private Field.Builder convertMap(String fieldName,
                                     org.apache.pulsar.shade.org.apache.avro.Schema avroFieldSchema) {
        Field keyField = Field.newBuilder(MAP_KEY_NAME, StandardSQLTypeName.STRING)
                                                        .setMode(Field.Mode.NULLABLE).build();
        Field valueField = convertField(MAP_VALUE_NAME, avroFieldSchema.getValueType()).get().build();
        return Field.newBuilder(fieldName,
                StandardSQLTypeName.STRUCT, keyField, valueField).setMode(Field.Mode.REPEATED);
    }
}