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

import static org.junit.Assert.assertEquals;
import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.FieldValueList;
import com.google.cloud.bigquery.Job;
import com.google.cloud.bigquery.JobId;
import com.google.cloud.bigquery.JobInfo;
import com.google.cloud.bigquery.QueryJobConfiguration;
import com.google.cloud.bigquery.TableResult;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import lombok.Cleanup;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.ecosystem.io.bigquery.BigQueryConfig;
import org.apache.pulsar.ecosystem.io.bigquery.testutils.AvroRecordsUtils;
import org.junit.Test;

/**
 * Integration test.
 */
public class AvroDataConvertTestIntegration {

    @Test
    public void testFirst() throws Exception {

        // 0. clean bigquery data.
        BigQueryConfig bigQueryConfig = new BigQueryConfig();
        bigQueryConfig.setProjectId("affable-ray-226821");
        bigQueryConfig.setDatasetName("integration");
        bigQueryConfig.setTableName("avro_table");
        BigQuery bigQuery = bigQueryConfig.createBigQuery();
        bigQuery.delete(bigQueryConfig.getTableId());
        Thread.sleep(5000);


        // 1. send some message.
        String pulsarTopic = "avro-bigquery-topic";
        String pulsarProducerName = "test-bigquery-produce-name";

        @Cleanup
        PulsarClient pulsarClient = PulsarClient.builder()
                .serviceUrl("pulsar://localhost:6650")
                .build();

        @Cleanup
        Producer<AvroRecordsUtils.Foo> pulsarProducer =
                pulsarClient.newProducer(Schema.AVRO(AvroRecordsUtils.Foo.class))
                        .topic(pulsarTopic)
                        .sendTimeout(100, TimeUnit.SECONDS)
                        .enableBatching(true)
                        .blockIfQueueFull(true)
                        .producerName(pulsarProducerName)
                        .create();

        for (int i = 0; i < 10; i++) {
            pulsarProducer.newMessage()
                    .sequenceId(i)
                    .eventTime(System.currentTimeMillis())
                    .value(AvroRecordsUtils.getFoo1()).send();
        }

        // 3. query and assert
        Thread.sleep(20000);
        TableResult tableResult = queryResult(bigQuery);
        assertEquals(10, tableResult.getTotalRows());
        for (FieldValueList fieldValues : tableResult.iterateAll()) {
            assertEquals(pulsarProducerName, fieldValues.get("__producer_name__").getStringValue());
            assertEquals("test col1", fieldValues.get("col1").getStringValue());
            assertEquals("test col3", fieldValues.get("col3").getStringValue());
        }
    }

    public TableResult queryResult(BigQuery bigQuery) throws InterruptedException {
        QueryJobConfiguration queryConfig =
                QueryJobConfiguration
                        .newBuilder("SELECT * FROM affable-ray-226821.integration.avro_table")
                        // Use standard SQL syntax for queries.
                        // See: https://cloud.google.com/bigquery/sql-reference/
                        .setUseLegacySql(false)
                        .build();

        // Create a job ID so that we can safely retry.
        JobId jobId = JobId.of(UUID.randomUUID().toString());
        Job queryJob = bigQuery.create(JobInfo.newBuilder(queryConfig).setJobId(jobId).build());

        // Wait for the query to complete.
        queryJob = queryJob.waitFor();

        // Check for errors
        if (queryJob == null) {
            throw new RuntimeException("Job no longer exists");
        } else if (queryJob.getStatus().getError() != null) {
            // You can also look at queryJob.getStatus().getExecutionErrors() for all
            // errors, not just the latest one.
            throw new RuntimeException(queryJob.getStatus().getError().toString());
        }

        // Get the results.
        TableResult result = queryJob.getQueryResults();
        return result;
    }
}
