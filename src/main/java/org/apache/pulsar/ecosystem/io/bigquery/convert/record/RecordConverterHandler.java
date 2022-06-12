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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.pulsar.client.api.schema.GenericObject;
import org.apache.pulsar.common.schema.SchemaType;
import org.apache.pulsar.ecosystem.io.bigquery.convert.schema.PrimitiveSchemaConvert;
import org.apache.pulsar.ecosystem.io.bigquery.exception.BigQueryConnectorRuntimeException;
import org.apache.pulsar.ecosystem.io.bigquery.exception.RecordConvertException;
import org.apache.pulsar.functions.api.Record;

/**
 * Record converter handler.
 */
public class RecordConverterHandler implements RecordConverter {

    private Map<SchemaType, RecordConverter> recordConverts = new HashMap<>();
    private PrimitiveRecordConvert primitiveRecordConvert;

    public RecordConverterHandler() {
        recordConverts.put(SchemaType.AVRO, new AvroRecordConverter());
        primitiveRecordConvert = new PrimitiveRecordConvert();
    }

    @Override
    public ProtoRows convertRecord(Record<GenericObject> record,
                                   Descriptors.Descriptor protoSchema,
                                   List<TableFieldSchema> tableFieldSchema) throws RecordConvertException {
        SchemaType type = record.getSchema().getSchemaInfo().getType();
        if (PrimitiveSchemaConvert.isPrimitiveSchema(type)) {
            return primitiveRecordConvert.convertRecord(record, protoSchema, tableFieldSchema);
        }
        RecordConverter recordConverter = recordConverts.get(type);
        if (recordConverter == null) {
            throw new BigQueryConnectorRuntimeException("Not support schema type: " + type);
        }
        return recordConverter.convertRecord(record, protoSchema, tableFieldSchema);
    }
}
