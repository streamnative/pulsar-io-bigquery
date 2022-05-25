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

import java.math.BigDecimal;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import lombok.Data;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.schema.GenericRecord;
import org.apache.pulsar.client.api.schema.GenericSchema;
import org.apache.pulsar.client.impl.schema.AvroSchema;
import org.apache.pulsar.client.impl.schema.generic.GenericAvroSchema;
import org.apache.pulsar.functions.api.Record;
import org.jetbrains.annotations.NotNull;
import org.mockito.Mockito;

/**
 * Big query all test.
 */
public class AvroRecordsUtils {

    @NotNull
    @SuppressWarnings("unchecked")
    public static Record<GenericRecord> getGenericRecordRecordFirst() {
        Dag dag = new Dag();
        dag.setTest("first data");
        dag.setDate(23456);
        dag.setBigDecimal(new BigDecimal("123.45"));
        dag.setTimeMillis(415988644);
        dag.setLocalTimesMillis(System.currentTimeMillis());
        dag.setTimestampMillis(System.currentTimeMillis());
        dag.setArray(Arrays.asList("a", "b", "c"));

        Map<String, Dag> map3 = new HashMap<>();
        map3.put("key1", dag);
        map3.put("key2", dag);

        Tag tag = new Tag();
        tag.booleanv = true;
        tag.bytesv = new byte[]{0, 1, 2};
        tag.bytev = "A".getBytes()[0];
        tag.charv = 'a';
        tag.doublev = 100.123;
        tag.floatv = (float) 123.2;
        tag.inta = 1;
        tag.intb = System.currentTimeMillis();

        Foo foo = new Foo();
        foo.setCol1("test col2");
        foo.setCol3("test col3");
        foo.setTag(tag);
        foo.setMap3(map3);

        AvroSchema<Foo> schema = AvroSchema.of(Foo.class);
        System.out.println(schema.getSchemaInfo());
        GenericSchema<GenericRecord> genericAvroSchema = new GenericAvroSchema(schema.getSchemaInfo());
        byte[] encode = schema.encode(foo);
        GenericRecord genericRecord = genericAvroSchema.decode(encode);

        MessageId messageId = Mockito.mock(MessageId.class);
        Mockito.when(messageId.toString()).thenReturn("1:1:123");
        Message message = Mockito.mock(Message.class);
        Mockito.when(message.getMessageId()).thenReturn(messageId);

        Record<GenericRecord> record = new Record<GenericRecord>() {

            @Override
            public Schema<GenericRecord> getSchema() {
                return genericAvroSchema;
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

            @Override
            public void ack() {
                System.out.println("ack message");
            }
        };
        return record;
    }

    @NotNull
    @SuppressWarnings("unchecked")
    public static Record<GenericRecord> getGenericRecordRecordSecond() {
        Dag dag = new Dag();
        dag.setTest("first data");
        dag.setDate(23456);
        dag.setBigDecimal(new BigDecimal("123.45"));
        dag.setTimeMillis(1000);
        dag.setLocalTimesMillis(System.currentTimeMillis());
        dag.setTimestampMillis(System.currentTimeMillis());
        dag.setArray(Arrays.asList("a", "b", "c"));

        Map<String, Dag> map3 = new HashMap<>();
        map3.put("key1", dag);
        map3.put("key2", dag);

        Tag tag = new Tag();
        tag.booleanv = true;
        tag.bytesv = new byte[]{0, 1, 2};
        tag.bytev = "A".getBytes()[0];
        tag.charv = 'a';
        tag.doublev = 100.123;
        tag.floatv = (float) 123.2;
        tag.inta = 1;
        tag.intb = System.currentTimeMillis();

        Foo2 foo = new Foo2();
        foo.setCol1("test col2");
        foo.setCol4("test col3");
        foo.setTag(tag);
        foo.setMap3(map3);

        AvroSchema<Foo2> schema = AvroSchema.of(Foo2.class);
        GenericSchema<GenericRecord> genericAvroSchema = new GenericAvroSchema(schema.getSchemaInfo());
        byte[] encode = schema.encode(foo);
        GenericRecord genericRecord = genericAvroSchema.decode(encode);

        MessageId messageId = Mockito.mock(MessageId.class);
        Mockito.when(messageId.toString()).thenReturn("1:1:123");

        Message message = Mockito.mock(Message.class);
        Mockito.when(message.getMessageId()).thenReturn(messageId);
        Record<GenericRecord> record = new Record<GenericRecord>() {

            @Override
            public Schema<GenericRecord> getSchema() {
                return genericAvroSchema;
            }

            @Override
            public Optional<Long> getEventTime() {
                return Optional.of(System.currentTimeMillis());
            }

            @Override
            public GenericRecord getValue() {
                return genericRecord;
            }

            @Override
            public Optional<Message<GenericRecord>> getMessage() {
                return Optional.of(message);
            }

            @Override
            public void ack() {
                System.out.println("ack message");
            }
        };
        return record;
    }

    @Data
    static class Foo {
        private String col1;
        private Tag tag;
        private String col3;
        private Map<String, Dag> map3;
    }

    @Data
    static class Foo2 {
        private String col1;
        private Tag tag;
        private String col4;
        private Map<String, Dag> map3;
    }

    static class Tag {
        private boolean booleanv;
        private double doublev;
        private float floatv;
        private int inta;
        private long intb;
        private byte[] bytesv;
        private byte bytev;
        private char charv;
    }

    @Data
    static class Dag {
        private String test;
        private List<String> array;
        @org.apache.avro.reflect.AvroSchema("{ \"type\": \"bytes\", \"logicalType\": \"decimal\", \"precision\": 5, "
                + "\"scale\": 2 }")
        private BigDecimal bigDecimal;
        @org.apache.avro.reflect.AvroSchema("{\n"
                + "  \"type\": \"int\",\n"
                + "  \"logicalType\": \"date\"\n"
                + "}")
        private int date;
        @org.apache.avro.reflect.AvroSchema("{\n"
                + "  \"type\": \"long\",\n"
                + "  \"logicalType\": \"local-timestamp-millis\"\n"
                + "}")
        private long localTimesMillis;
        @org.apache.avro.reflect.AvroSchema("{\n"
                + "  \"type\": \"long\",\n"
                + "  \"logicalType\": \"time-micros\"\n"
                + "}")
        private long timeMillis;
        @org.apache.avro.reflect.AvroSchema("{\n"
                + "  \"type\": \"long\",\n"
                + "  \"logicalType\": \"timestamp-millis\"\n"
                + "}")
        private long timestampMillis;
    }
}
