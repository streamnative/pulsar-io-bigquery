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

import com.google.cloud.bigquery.storage.v1.ProtoRows;
import com.google.cloud.bigquery.storage.v1.TableFieldSchema;
import com.google.protobuf.Descriptors;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.UninitializedMessageException;
import java.util.List;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.client.api.schema.GenericObject;
import org.apache.pulsar.ecosystem.io.bigquery.convert.DefaultSystemFieldConvert;
import org.apache.pulsar.ecosystem.io.bigquery.exception.RecordConvertException;
import org.apache.pulsar.functions.api.Record;

/**
 * Abstract record convert.
 */
@Slf4j
public abstract class AbstractRecordConvert implements RecordConverter {

    @Override
    public ProtoRows convertRecord(Record<GenericObject> record, Descriptors.Descriptor protoSchema,
                                   List<TableFieldSchema> tableFieldSchema) throws RecordConvertException {
        GenericObject genericObject = record.getValue();
        ProtoRows.Builder rowsBuilder = ProtoRows.newBuilder();
        DynamicMessage.Builder protoMsg = DynamicMessage.newBuilder(protoSchema);
        convertSystemField(protoMsg, record, protoSchema, tableFieldSchema);
        convertUserField(protoMsg, genericObject.getNativeObject(), protoSchema, tableFieldSchema);
        DynamicMessage dynamicMessage = buildMessage(protoMsg);
        rowsBuilder.addSerializedRows(dynamicMessage.toByteString());
        return rowsBuilder.build();
    }

    abstract DynamicMessage.Builder convertUserField(DynamicMessage.Builder protoMsg, Object nativeRecord,
                                               Descriptors.Descriptor protoDescriptor,
                                               List<TableFieldSchema> tableFieldSchema) throws RecordConvertException;


    private void convertSystemField(DynamicMessage.Builder protoMsg, Record<GenericObject> record,
                                    Descriptors.Descriptor protoDescriptor,
                                    List<TableFieldSchema> tableFieldSchema) {
        for (Descriptors.FieldDescriptor pbField : protoDescriptor.getFields()) {
            TableFieldSchema fieldSchema = tableFieldSchema.get(pbField.getIndex());
            String fieldName = fieldSchema.getName();
            if (record != null && DefaultSystemFieldConvert.isSystemField(fieldName)) {
                try {
                    Object convert = DefaultSystemFieldConvert.convert(fieldName, record);
                    protoMsg.setField(pbField, convert);
                } catch (Exception e) {
                    log.warn("Not found field <{}> by records, ignore this field", fieldName);
                    continue;
                }
            }
        }
    }

    protected DynamicMessage buildMessage(DynamicMessage.Builder protoMsg) throws RecordConvertException {
        try {
            return protoMsg.build();
        } catch (UninitializedMessageException e) {
            String errorMsg = e.getMessage();
            int idxOfColon = errorMsg.indexOf(":");
            String missingFieldName = errorMsg.substring(idxOfColon + 2);
            throw new RecordConvertException(
                    String.format(
                            "Avro does not have the required field %s.%s.", "todo", missingFieldName));
        }
    }
}
