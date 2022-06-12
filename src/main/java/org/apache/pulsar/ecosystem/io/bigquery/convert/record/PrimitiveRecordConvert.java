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
import com.google.protobuf.Descriptors;
import com.google.protobuf.DynamicMessage;
import java.util.List;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.client.api.schema.GenericObject;
import org.apache.pulsar.common.schema.SchemaType;
import org.apache.pulsar.ecosystem.io.bigquery.convert.DefaultSystemFieldConvert;
import org.apache.pulsar.ecosystem.io.bigquery.convert.logicaltype.PrimitiveLogicalFieldConvert;
import org.apache.pulsar.ecosystem.io.bigquery.exception.BigQueryConnectorRuntimeException;
import org.apache.pulsar.functions.api.Record;

/**
 * primitive record convert.
 */
@Slf4j
public class PrimitiveRecordConvert extends AbstractRecordConvert {

    private final PrimitiveLogicalFieldConvert primitiveLogicalFieldConvert;

    public PrimitiveRecordConvert() {
        this.primitiveLogicalFieldConvert = new PrimitiveLogicalFieldConvert();
    }

    @Override
    protected DynamicMessage.Builder convertUserField(DynamicMessage.Builder protoMsg, Record<GenericObject> record,
                                            Descriptors.Descriptor protoDescriptor,
                                            List<TableFieldSchema> tableFieldSchema) {
        for (Descriptors.FieldDescriptor field : protoDescriptor.getFields()) {
            if (field.getName().equals(DefaultSystemFieldConvert.PRIMITIVE_VALUE_NAME)) {
                protoMsg.setField(field, convertField(record));
            }
        }
        return protoMsg;
    }

    private Object convertField(Record<GenericObject> record) {
        SchemaType type = record.getSchema().getSchemaInfo().getType();
        Object nativeObject = record.getValue().getNativeObject();
        if (primitiveLogicalFieldConvert.isLogicType(type)) {
            return primitiveLogicalFieldConvert.convertFieldValue(type, nativeObject);
        } else {
            switch (type) {
                case STRING:
                case BOOLEAN:
                case DOUBLE:
                case BYTES:
                case INT64:
                    return nativeObject;
                case INT8:
                    if (nativeObject instanceof Byte) {
                        return ((Byte) nativeObject).longValue();
                    }
                    break;
                case INT16:
                    if (nativeObject instanceof Short) {
                        return ((Short) nativeObject).longValue();
                    }
                    break;
                case INT32:
                    if (nativeObject instanceof Integer) {
                        return ((Integer) nativeObject).longValue();
                    }
                    break;
                case FLOAT:
                    if (nativeObject instanceof Float) {
                        return ((Float) nativeObject).doubleValue();
                    }
                    break;
            }
            throw new BigQueryConnectorRuntimeException("Not support type: " + type
                    + ", data class: " + nativeObject.getClass().getName());
        }
    }
}
