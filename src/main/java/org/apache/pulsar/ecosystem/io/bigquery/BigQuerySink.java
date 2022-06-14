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
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.client.api.schema.GenericObject;
import org.apache.pulsar.ecosystem.io.bigquery.convert.record.RecordConverter;
import org.apache.pulsar.ecosystem.io.bigquery.convert.record.RecordConverterHandler;
import org.apache.pulsar.ecosystem.io.bigquery.exception.BigQueryConnectorRuntimeException;
import org.apache.pulsar.ecosystem.io.bigquery.exception.RecordConvertException;
import org.apache.pulsar.functions.api.Record;
import org.apache.pulsar.io.core.Sink;
import org.apache.pulsar.io.core.SinkContext;

/**
 * BigQuery Sink impl.
 */
@Slf4j
public class BigQuerySink implements Sink<GenericObject> {

    // data writer
    private DataWriterBatchWrapper dataWriterBatch;
    // pulsar
    private RecordConverter recordConverter;
    private BigQueryConfig bigQueryConfig;
    private SchemaManager schemaManager;

    // is init bq resources
    private boolean init;

    // All operations inside bigquery are handled by this separate thread
    private ScheduledExecutorService scheduledExecutorService;
    private Executor callBackThread;

    private volatile Exception error;

    @Override
    public void open(Map<String, Object> config, SinkContext sinkContext) throws Exception {
        this.bigQueryConfig = BigQueryConfig.load(config, sinkContext);
        Objects.requireNonNull(bigQueryConfig.getProjectId(), "BigQuery project id is not set");
        Objects.requireNonNull(bigQueryConfig.getDatasetName(), "BigQuery dataset id is not set");
        Objects.requireNonNull(bigQueryConfig.getTableName(), "BigQuery table name id is not set");

        this.recordConverter = new RecordConverterHandler();
        this.schemaManager = new SchemaManager(bigQueryConfig);
        this.scheduledExecutorService =
                Executors.newSingleThreadScheduledExecutor(new DefaultThreadFactory("bigquery-sink"));
        this.callBackThread = Executors.newSingleThreadScheduledExecutor(
                new DefaultThreadFactory("bigquery-sink-callback"));
        DataWriter dataWriter;
        if (bigQueryConfig.getVisibleModel() == BigQueryConfig.VisibleModel.Committed) {
            dataWriter = new DataWriterCommitted(bigQueryConfig.createBigQueryWriteClient(),
                    bigQueryConfig.getTableName());
        } else {
            dataWriter = new DataWriterPending(bigQueryConfig.createBigQueryWriteClient(),
                    bigQueryConfig.getTableName());
        }
        this.dataWriterBatch = new DataWriterBatchWrapper(dataWriter, schemaManager, callBackThread,
                10, 3000);
    }

    @Override
    public void write(Record<GenericObject> record) throws Exception {
        // An unrecoverable exception was encountered during processing
        if (error != null) {
            throw error;
        }
        useInnerThreadHandle(record).get();
    }

    /**
     * Use internal threads to process messages, and batch timers are one thread to avoid thread safety.
     *
     * @param record
     * @return
     */
    private CompletableFuture<Void> useInnerThreadHandle(Record<GenericObject> record) {
        return CompletableFuture.runAsync(() -> {
            // 0. Try to create table and init bigquery resources
            if (!init) {
                schemaManager.initTable(record);
                try {
                    dataWriterBatch.updateResources(schemaManager.getProtoSchema());
                } catch (IOException e) {
                    throw new BigQueryConnectorRuntimeException(e);
                }
                this.scheduledExecutorService.scheduleAtFixedRate(() -> writeData(null),
                        5, 5, TimeUnit.SECONDS);
                init = true;
            }

            // 1. convert record and try update schema.
            DynamicMessage msg = convertRecord(record);

            // 2. append msg.
            writeData(Arrays.asList(new DataWriter.DataWriterRequest(msg, record)));

        }, scheduledExecutorService);
    }

    private void writeData(List<DataWriter.DataWriterRequest> dataWriterRequests) {
        try {
            dataWriterBatch.append(dataWriterRequests);
        } catch (Exception e) {
            this.error = e;
        }
    }

    private DynamicMessage convertRecord(Record<GenericObject> record) {
        try {
            return recordConverter.convertRecord(record, schemaManager.getDescriptor(),
                    schemaManager.getTableSchema().getFieldsList());
        } catch (RecordConvertException e) {
            // Not care why exception, try to update the schema directly and get the latest tableSchema.
            log.warn("Convert failed to record, try update schema: <{}>", e.getMessage());
            schemaManager.updateSchema(record);
        }

        // Bigquery resource update is delayed, try a few more times.
        try {
            dataWriterBatch.updateResources(schemaManager.getProtoSchema());
            return recordConverter.convertRecord(record, schemaManager.getDescriptor(),
                    schemaManager.getTableSchema().getFieldsList());
        } catch (Exception e) {
            throw new BigQueryConnectorRuntimeException(
                    "Convert record failed, after trying to update the schema it still fails", e);
        }
    }

    @Override
    public void close() throws Exception {
        dataWriterBatch.close();
    }
}
