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
package org.apache.pulsar.ecosystem.io.bigquery.samples;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import lombok.Data;
import org.apache.pulsar.client.api.schema.GenericRecord;
import org.apache.pulsar.client.api.schema.GenericSchema;
import org.apache.pulsar.client.impl.schema.AvroSchema;
import org.apache.pulsar.client.impl.schema.generic.GenericAvroSchema;
import org.apache.pulsar.ecosystem.io.bigquery.BigQueryConfig;
import org.apache.pulsar.ecosystem.io.bigquery.SchemaManager;
import org.apache.pulsar.functions.api.Record;

/**
 * Schema manager test.
 */
public class SchemaManagerTest {

    public static void main(String[] args) throws IOException {
        SchemaManagerTest schemaManagerTest = new SchemaManagerTest();
        schemaManagerTest.testCreateTable();
    }

    public void testCreateTable() throws IOException {
        BigQueryConfig bigQueryConfig = new BigQueryConfig();
        bigQueryConfig.setProjectId("affable-ray-226821");
        bigQueryConfig.setDatasetName("testdset");
        bigQueryConfig.setTableName("auto_create");
        SchemaManager schemaManager = new SchemaManager(bigQueryConfig);
        schemaManager.createTable(getRecord());
    }

    public void testUpdateTable() throws IOException {
        BigQueryConfig bigQueryConfig = new BigQueryConfig();
        bigQueryConfig.setProjectId("affable-ray-226821");
        bigQueryConfig.setDatasetName("testdset");
        bigQueryConfig.setTableName("test_required");
        SchemaManager schemaManager = new SchemaManager(bigQueryConfig);
        schemaManager.updateSchema(getRecord2());
    }

    public Record<GenericRecord> getRecord2() {
        TestMessage testMessage = new TestMessage();
        testMessage.setCol1("col1");
        AvroSchema<TestMessage> schema = AvroSchema.of(TestMessage.class);
        GenericSchema<GenericRecord> genericAvroSchema = new GenericAvroSchema(schema.getSchemaInfo());

        byte[] encode = schema.encode(testMessage);

        GenericRecord genericRecord = genericAvroSchema.decode(encode);

        Record<GenericRecord> record = new Record<GenericRecord>() {
            @Override
            public GenericRecord getValue() {
                return genericRecord;
            }
        };
        return record;
    }

    public Record<GenericRecord> getRecord() {
        Dag dag = new Dag();
        dag.setTest("dsds");

        Map<String, Dag> map3 = new HashMap<>();
        map3.put("key1", dag);
        map3.put("key2", dag);


        Foo foo = new Foo();
        foo.setCol4("test col4");
        foo.setCol3("test col3");
        foo.setCol2(dag);
        foo.setMap3(map3);
        foo.setMap4(map3);

        AvroSchema<Foo> schema = AvroSchema.of(Foo.class);
        GenericSchema<GenericRecord> genericAvroSchema = new GenericAvroSchema(schema.getSchemaInfo());

        byte[] encode = schema.encode(foo);

        GenericRecord genericRecord = genericAvroSchema.decode(encode);

        Record<GenericRecord> record = new Record<GenericRecord>() {
            @Override
            public GenericRecord getValue() {
                return genericRecord;
            }
        };
        return record;
    }

    @Data
    static class TestMessage {
        private String col1;
        private Dag col2;
    }

    @Data
    static class Foo {
        private String col4;
        private Dag col2;
        private String col3;
        private Map<String, Dag> map3;
        private Map<String, Dag> map4;
    }

    @Data
    static class Dag {
        private String test;
    }
}
