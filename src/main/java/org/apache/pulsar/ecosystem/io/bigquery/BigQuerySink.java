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

import com.google.protobuf.DynamicMessage;
import io.netty.util.concurrent.DefaultThreadFactory;
import java.util.Arrays;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.client.api.schema.GenericObject;
import org.apache.pulsar.ecosystem.io.bigquery.convert.record.RecordConverter;
import org.apache.pulsar.ecosystem.io.bigquery.convert.record.RecordConverterHandler;
import org.apache.pulsar.ecosystem.io.bigquery.exception.BQConnectorDirectFailException;
import org.apache.pulsar.ecosystem.io.bigquery.exception.BQConnectorRecordConvertException;
import org.apache.pulsar.functions.api.Record;
import org.apache.pulsar.io.core.Sink;
import org.apache.pulsar.io.core.SinkContext;

/**
 * BigQuery Sink impl.
 */
@Slf4j
public class BigQuerySink implements Sink<GenericObject> {

    // data writer
    private DataWriter dataWriterBatch;
    // pulsar
    private RecordConverter recordConverter;
    private BigQueryConfig bigQueryConfig;
    private SchemaManager schemaManager;

    // is init bq resources
    private boolean init;

    // All operations inside bigquery are handled by this separate thread
    private ScheduledExecutorService scheduledExecutorService;

    private SinkContext sinkContext;

    public BigQuerySink() {
    }

    /**
     * Only use by unit test.
     */
    protected BigQuerySink(DataWriterBatchWrapper dataWriterBatch, RecordConverter recordConverter,
                        BigQueryConfig bigQueryConfig, SchemaManager schemaManager, SinkContext sinkContext) {
        this.dataWriterBatch = dataWriterBatch;
        this.recordConverter = recordConverter;
        this.bigQueryConfig = bigQueryConfig;
        this.schemaManager = schemaManager;
        this.scheduledExecutorService =
                Executors.newSingleThreadScheduledExecutor(new DefaultThreadFactory("bigquery-sink"));
        this.sinkContext = sinkContext;
    }

    @Override
    public void open(Map<String, Object> config, SinkContext sinkContext) {
        this.bigQueryConfig = BigQueryConfig.load(config, sinkContext);
        Objects.requireNonNull(bigQueryConfig.getProjectId(), "BigQuery project id is not set");
        Objects.requireNonNull(bigQueryConfig.getDatasetName(), "BigQuery dataset id is not set");
        Objects.requireNonNull(bigQueryConfig.getTableName(), "BigQuery table name id is not set");

        this.recordConverter = new RecordConverterHandler();
        this.schemaManager = new SchemaManager(bigQueryConfig);
        this.scheduledExecutorService =
                Executors.newSingleThreadScheduledExecutor(new DefaultThreadFactory("bigquery-sink"));
        DataWriter dataWriter;
        if (bigQueryConfig.getVisibleModel() == BigQueryConfig.VisibleModel.Committed) {
            dataWriter = new DataWriterCommitted(bigQueryConfig.createBigQueryWriteClient(), schemaManager, sinkContext,
                    bigQueryConfig.getTableName(), bigQueryConfig.getFailedMaxRetryNum());
        } else if (bigQueryConfig.getVisibleModel() == BigQueryConfig.VisibleModel.Pending) {
            dataWriter = new DataWriterPending(bigQueryConfig.createBigQueryWriteClient(), schemaManager, sinkContext,
                    bigQueryConfig.getTableName(), bigQueryConfig.getFailedMaxRetryNum(), 10000);
        } else {
            throw new BQConnectorDirectFailException("Not support visible model: " + bigQueryConfig.getVisibleModel()
                    + ", support Committed or Pending");
        }
        this.dataWriterBatch = new DataWriterBatchWrapper(dataWriter, bigQueryConfig.getBatchMaxSize(),
                bigQueryConfig.getBatchMaxTime(), bigQueryConfig.getBatchFlushIntervalTime(),
                scheduledExecutorService, sinkContext);
        this.sinkContext = sinkContext;
    }

    @Override
    public void write(Record<GenericObject> record) throws Exception {
        useInnerThreadHandle(record);
    }

    /**
     * Use internal threads to process messages, and batch timers are one thread to avoid thread safety.
     *
     * @param record
     * @return
     */
    private void useInnerThreadHandle(Record<GenericObject> record) throws Exception {
        CompletableFuture.runAsync(() -> {
            // 0. Try to create table and init bigquery resources
            if (!init) {
                schemaManager.initTable(record);
                dataWriterBatch.updateStream(schemaManager.getProtoSchema());
                init = true;
            }

            // 1. convert record and try update schema.
            DynamicMessage msg = convertRecord(record);

            // 2. append msg.
            dataWriterBatch.append(Arrays.asList(new DataWriter.DataWriterRequest(msg, record)));

        }, scheduledExecutorService).get();
    }

    private DynamicMessage convertRecord(Record<GenericObject> record) {
        try {
            return recordConverter.convertRecord(record, schemaManager.getDescriptor(),
                    schemaManager.getTableSchema().getFieldsList());
        } catch (BQConnectorRecordConvertException e) {
            // Not care why exception, try to update the schema directly and get the latest tableSchema.
            log.warn("Convert failed to record, try update schema: <{}>", e.getMessage());
            try {
                schemaManager.updateSchema(record);
                // Bigquery resource update is delayed, try a few more times.
                dataWriterBatch.updateStream(schemaManager.getProtoSchema());
                sinkContext.recordMetric(MetricContent.UPDATE_SCHEMA_COUNT, 1);
                return recordConverter.convertRecord(record, schemaManager.getDescriptor(),
                        schemaManager.getTableSchema().getFieldsList());
            } catch (Exception ex) {
                throw new BQConnectorDirectFailException(
                        "Convert record failed, after trying to update the schema it still fails", ex);
            }
        }

    }

    @Override
    public void close() throws Exception {
        dataWriterBatch.close();
    }
}
