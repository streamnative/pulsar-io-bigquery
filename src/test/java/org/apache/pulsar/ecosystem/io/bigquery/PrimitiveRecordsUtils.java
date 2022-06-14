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

import java.util.Optional;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.schema.GenericObject;
import org.apache.pulsar.client.api.schema.GenericRecord;
import org.apache.pulsar.client.impl.schema.AutoConsumeSchema;
import org.apache.pulsar.common.protocol.schema.SchemaVersion;
import org.apache.pulsar.functions.api.Record;
import org.jetbrains.annotations.NotNull;
import org.mockito.Mockito;

/**
 * primitive records utils.
 */
public class PrimitiveRecordsUtils {

    @NotNull
    @SuppressWarnings("unchecked")
    public static Record<GenericObject> getRecord(Schema schema, Object value) {
        AutoConsumeSchema autoConsumeSchema = new AutoConsumeSchema();
        autoConsumeSchema.setSchema(schema);
        GenericRecord genericRecord = AutoConsumeSchema.wrapPrimitiveObject(value,
                                      schema.getSchemaInfo().getType(), SchemaVersion.Latest.bytes());
        MessageId messageId = Mockito.mock(MessageId.class);
        Mockito.when(messageId.toString()).thenReturn("1:1:123");
        Message message = Mockito.mock(Message.class);
        Mockito.when(message.getMessageId()).thenReturn(messageId);
        Record<? extends GenericObject> record = new Record<GenericRecord>() {
            @Override
            public Schema<GenericRecord> getSchema() {
                return autoConsumeSchema;
            }

            @Override
            public GenericRecord getValue() {
                return genericRecord;
            }

            @Override
            public Optional<Long> getEventTime() {
                return Optional.of(System.currentTimeMillis());
            }

            @Override
            public Optional<Message<GenericRecord>> getMessage() {
                return Optional.of(message);
            }
        };
        return (Record<GenericObject>) record;
    }

}
