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
package org.apache.pulsar.ecosystem.io.bigquery.integration;

import java.util.HashMap;
import java.util.Map;
import org.apache.pulsar.client.api.schema.GenericObject;
import org.apache.pulsar.ecosystem.io.bigquery.BigQuerySink;
import org.apache.pulsar.ecosystem.io.bigquery.testutils.AvroRecordsUtils;
import org.apache.pulsar.functions.api.Record;
import org.apache.pulsar.io.core.SinkContext;
import org.mockito.Mockito;

/**
 * Big query all test. Just use the local debug test.
 */
public class BigQuerySinkAllTestAvro {

    public static void main(String[] args) throws Exception {
        BigQuerySinkAllTestAvro bigQuerySinkAllTestNew = new BigQuerySinkAllTestAvro();
        bigQuerySinkAllTestNew.testBase();
    }

    public void testBase() throws Exception {

        BigQuerySink bigQuerySink = new BigQuerySink();

        Map<String, Object> sinkConfig = new HashMap<>();
        sinkConfig.put("projectId", "affable-ray-226821");
        sinkConfig.put("datasetName", "testdset");
        sinkConfig.put("tableName", "all_table");
        sinkConfig.put("autoCreateTable", true);
        sinkConfig.put("autoUpdateTable", true);
        sinkConfig.put("clusteredTables", true);
        sinkConfig.put("partitionedTables", true);
        sinkConfig.put("partitionedTableIntervalDay", 10);
        sinkConfig.put("defaultSystemField", "__event_time__");
        sinkConfig.put("visibleModel", "Committed");
        sinkConfig.put("batchMaxSize", "20");
        sinkConfig.put("batchMaxTime", "5000");
        sinkConfig.put("batchFlushIntervalTime", "2");
        sinkConfig.put("failedMaxRetryNum", "10");
        bigQuerySink.open(sinkConfig, Mockito.mock(SinkContext.class));

        for (int i = 0; i < 100000; i++) {
            Record<GenericObject> genericRecordRecordSecond = AvroRecordsUtils.getGenericRecordRecordSecond();
            bigQuerySink.write(genericRecordRecordSecond);
        }

        // write async
        Thread.sleep(50000);
        bigQuerySink.close();
        System.exit(0);
    }


}
