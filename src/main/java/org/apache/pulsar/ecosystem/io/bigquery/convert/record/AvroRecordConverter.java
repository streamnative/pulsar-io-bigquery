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
package org.apache.pulsar.ecosystem.io.bigquery.convert.record;

import com.google.cloud.bigquery.storage.v1.TableFieldSchema;
import com.google.protobuf.ByteString;
import com.google.protobuf.Descriptors;
import com.google.protobuf.DynamicMessage;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.util.Utf8;
import org.apache.pulsar.client.api.schema.GenericObject;
import org.apache.pulsar.ecosystem.io.bigquery.convert.DefaultSystemFieldConvert;
import org.apache.pulsar.ecosystem.io.bigquery.convert.logicaltype.AvroLogicalFieldConvert;
import org.apache.pulsar.ecosystem.io.bigquery.exception.BigQueryConnectorRuntimeException;
import org.apache.pulsar.ecosystem.io.bigquery.exception.RecordConvertException;
import org.apache.pulsar.functions.api.Record;

/**
 * avro record converter.
 */
@Slf4j
public class AvroRecordConverter extends AbstractRecordConvert {

    private AvroLogicalFieldConvert logicalFieldConvert;

    public AvroRecordConverter() {
        logicalFieldConvert = new AvroLogicalFieldConvert();
    }

    @Override
    protected DynamicMessage.Builder convertUserField(DynamicMessage.Builder protoMsg, Record<GenericObject> record,
                                                      Descriptors.Descriptor protoDescriptor,
                                                      List<TableFieldSchema> tableFieldSchema)
                                                                                      throws RecordConvertException {
        return convertUserField(protoMsg, record.getValue().getNativeObject(), protoDescriptor, tableFieldSchema);
    }

    protected DynamicMessage.Builder convertUserField(DynamicMessage.Builder protoMsg, Object nativeRecord,
                                      Descriptors.Descriptor protoDescriptor,
                                      List<TableFieldSchema> tableFieldSchema) throws RecordConvertException {
        org.apache.avro.generic.GenericRecord avroNativeRecord =
                (org.apache.avro.generic.GenericRecord) nativeRecord;
        List<String> pulsarFields = avroNativeRecord.getSchema().getFields().
                stream().map(field -> field.name()).collect(Collectors.toList());
        Schema avroSchema = avroNativeRecord.getSchema();
        for (Descriptors.FieldDescriptor pbField : protoDescriptor.getFields()) {
            TableFieldSchema fieldSchema = tableFieldSchema.get(pbField.getIndex());
            String fieldName = fieldSchema.getName();
            Object value;
            Schema avroFieldSchema;
            try {
                value = avroNativeRecord.get(fieldName);
                avroFieldSchema = pickSchema(avroSchema.getField(fieldName).schema());
            } catch (Exception e) {
                if (!DefaultSystemFieldConvert.isSystemField(fieldName)) {
                    log.warn("Not found field <{}> by avro data, ignore this field", fieldName);
                }
                continue;
            }
            if (pbField.isRepeated()) {
                fillRepeatedField(protoMsg, avroFieldSchema, value, pbField, fieldSchema);
            } else {
                fillField(protoMsg, avroFieldSchema, value, pbField, fieldSchema);
            }
            pulsarFields.remove(fieldName);
        }
        if (!pulsarFields.isEmpty()) {
            throw new RecordConvertException("Convert exception, "
                    + "pulsar have some field not found by big query: "
                    + pulsarFields);
        }
        return protoMsg;
    }

    private void fillRepeatedField(DynamicMessage.Builder protoMsg, Schema avroFieldSchema, Object value,
                                   Descriptors.FieldDescriptor pbFieldDescriptor,
                                   TableFieldSchema tableFieldSchema) throws RecordConvertException {
        if (value instanceof Map<?, ?>) {
            Map<?, ?> map = (HashMap<?, ?>) value;
            for (Map.Entry<?, ?> entry : map.entrySet()) {
                Object mapKey = entry.getKey();
                Object mapValue = entry.getValue();
                Descriptors.Descriptor mapDescriptor = pbFieldDescriptor.getMessageType();
                DynamicMessage.Builder mapPbMsg = DynamicMessage.newBuilder(mapDescriptor);
                Descriptors.FieldDescriptor keyDescriptor = mapDescriptor.
                        findFieldByName(DefaultSystemFieldConvert.MAP_KEY_NAME);
                Descriptors.FieldDescriptor valueDescriptor = mapDescriptor.
                        findFieldByName(DefaultSystemFieldConvert.MAP_VALUE_NAME);
                TableFieldSchema keyFieldSchema = tableFieldSchema.getFields(keyDescriptor.getIndex());
                TableFieldSchema valueFieldSchema = tableFieldSchema.getFields(valueDescriptor.getIndex());
                // Map key type is string, Array if key is not equal to string
                fillField(mapPbMsg, Schema.create(Schema.Type.STRING), mapKey, keyDescriptor, keyFieldSchema);
                fillField(mapPbMsg, pickSchema(avroFieldSchema).getValueType(),
                        mapValue, valueDescriptor, valueFieldSchema);
                protoMsg.addRepeatedField(pbFieldDescriptor, buildMessage(mapPbMsg));
            }
            map.forEach((mapKey, mapValue) -> {
            });
        } else if (value instanceof GenericData.Array) {
            // list and set
            GenericData.Array array = (GenericData.Array) value;
            for (Object arrayObject : array) {
                fillField(protoMsg, pickSchema(avroFieldSchema).getElementType(),
                        arrayObject, pbFieldDescriptor, tableFieldSchema);
            }
        } else {
            throw new RecordConvertException("Not support repeated type: " + value.getClass());
        }
    }


    private void fillField(DynamicMessage.Builder protoMsg, Schema avroFieldSchema, Object value,
                           Descriptors.FieldDescriptor pbFieldDescriptor, TableFieldSchema tableFieldSchema)
            throws RecordConvertException {
        TableFieldSchema.Type type = tableFieldSchema.getType();
        if (avroFieldSchema.getLogicalType() != null
                && logicalFieldConvert.isLogicType(avroFieldSchema.getLogicalType())) {
            if (log.isDebugEnabled()) {
                log.debug("Convert logical type " + avroFieldSchema);
            }
            protoMsg.setField(pbFieldDescriptor,
                    logicalFieldConvert.convertFieldValue(avroFieldSchema.getLogicalType(), value));
        } else {
            switch (type) {
                case STRING:
                case BOOL:
                case DOUBLE:
                case BYTES:
                case INT64:
                    if (value instanceof Utf8) {
                        appendField(protoMsg, pbFieldDescriptor, ((Utf8) value).toString());
                        return;
                    }
                    if (value instanceof Integer) {
                        appendField(protoMsg, pbFieldDescriptor, ((Integer) value).longValue());
                        return;
                    }
                    if (value instanceof ByteBuffer) {
                        appendField(protoMsg, pbFieldDescriptor, ByteString.copyFrom((ByteBuffer) value));
                        return;
                    }
                    if (value instanceof Float) {
                        appendField(protoMsg, pbFieldDescriptor, ((Float) value).doubleValue());
                        return;
                    }
                    appendField(protoMsg, pbFieldDescriptor, value);
                    return;
                case STRUCT:
                    // If type is STRUCT, continue recursion
                    if (value instanceof org.apache.avro.generic.GenericRecord) {
                        DynamicMessage structMsg =
                                buildMessage(
                                        convertUserField(DynamicMessage.newBuilder(pbFieldDescriptor.getMessageType()),
                                                value,
                                                pbFieldDescriptor.getMessageType(), tableFieldSchema.getFieldsList()));
                        appendField(protoMsg, pbFieldDescriptor, structMsg);
                        return;
                    }
                    break;
            }
            log.warn("Not support type <{}> valueClass <{}>, ignore this failed<{}>",
                                                             type, value.getClass(), avroFieldSchema.getName());
        }
    }

    private void appendField(DynamicMessage.Builder protoMsg,
                             Descriptors.FieldDescriptor pbFieldDescriptor, Object value) {
        if (pbFieldDescriptor.isRepeated()) {
            protoMsg.addRepeatedField(pbFieldDescriptor, value);
        } else {
            protoMsg.setField(pbFieldDescriptor, value);
        }
    }


    /**
     * For union types, take the first non-null type.
     * Reference avro docs: https://avro.apache.org/docs/current/spec.html#Unions
     *
     * @param avroFieldSchema
     * @return
     */
    public static Schema pickSchema(Schema avroFieldSchema) {
        if (avroFieldSchema.isUnion()) {
            for (Schema type : avroFieldSchema.getTypes()) {
                if (!type.isNullable()) {
                    return type;
                }
            }
            throw new BigQueryConnectorRuntimeException("There are no types in composite types that are not NULL");
        }
        return avroFieldSchema;
    }

}
