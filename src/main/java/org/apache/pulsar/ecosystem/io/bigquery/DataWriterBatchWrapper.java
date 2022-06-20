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

import com.google.api.gax.rpc.InternalException;
import com.google.api.gax.rpc.NotFoundException;
import com.google.api.gax.rpc.ResourceExhaustedException;
import com.google.api.gax.rpc.UnavailableException;
import com.google.api.gax.rpc.UnknownException;
import com.google.cloud.bigquery.storage.v1.ProtoSchema;
import com.google.common.util.concurrent.Uninterruptibles;
import com.google.protobuf.DynamicMessage;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.client.api.schema.GenericObject;
import org.apache.pulsar.ecosystem.io.bigquery.exception.BQConnectorDirectFailException;
import org.apache.pulsar.functions.api.Record;

/**
 * Temporarily used as a wrapper for batch processing,
 * and remove it after waiting for Sink to support batch processing.
 */
@Slf4j
public class DataWriterBatchWrapper {

    private final DataWriter dataWriter;
    private final int batchMaxSize;
    private final int batchMaxTime;
    // Maximum number of retries after encountering a retryable exception
    private final int failedMaxRetryNum;
    private final List<DataWriter.DataWriterRequest> batch;
    private final int batchFlushIntervalTime;
    private final SchemaManager schemaManager;
    private final ScheduledExecutorService scheduledExecutorService;
    private long lastFlushTime;

    /**
     * Batch writer wrapper.
     *
     * @param dataWriter {@link DataWriter}
     * @param schemaManager {@link SchemaManager}
     * @param batchMaxSize Batch max size.
     * @param batchMaxTime Batch max wait time.
     * @param batchFlushIntervalTime Batch trigger flush interval time: milliseconds
     * @param failedMaxRetryNum When append failed, max retry num.
     * @param scheduledExecutorService Schedule flush thread, this class does not guarantee thread safety,
     *                                 you need to ensure that the thread calling append is this thread.
     */
    public DataWriterBatchWrapper(DataWriter dataWriter, SchemaManager schemaManager,
                                  int batchMaxSize, int batchMaxTime, int batchFlushIntervalTime,
                                  int failedMaxRetryNum, ScheduledExecutorService scheduledExecutorService) {
        this.dataWriter = dataWriter;
        this.schemaManager = schemaManager;
        this.batchMaxSize = batchMaxSize;
        this.batchMaxTime = batchMaxTime;
        this.batchFlushIntervalTime = batchFlushIntervalTime;
        this.failedMaxRetryNum = failedMaxRetryNum;
        this.scheduledExecutorService = scheduledExecutorService;
        this.batch = new ArrayList<>(batchMaxSize);
    }

    /**
     * start scheduled trigger flush timeout data.
     */
    public void init() {
        this.lastFlushTime = System.currentTimeMillis();
        log.info("Start timed trigger refresh service, batchMaxSize:[{}], "
                + "batchMaxTime:[{}] batchFlushIntervalTime:[{}]", batchMaxSize, batchMaxTime, batchFlushIntervalTime);
        this.scheduledExecutorService.scheduleAtFixedRate(() -> tryFlush(),
                batchFlushIntervalTime, batchFlushIntervalTime, TimeUnit.MILLISECONDS);
    }

    public void append(DataWriter.DataWriterRequest dataWriterRequests) {
        batch.add(dataWriterRequests);
        tryFlush();
    }

    private void tryFlush() {
        if (batch.size() > 0
                && (batch.size() >= batchMaxSize || System.currentTimeMillis() - lastFlushTime > batchMaxTime)) {
            log.info("flush size {}", batch.size());
            List<DynamicMessage> dynamicMessages = batch.stream()
                    .map(dataWriterRequest -> dataWriterRequest.getDynamicMessage())
                    .collect(Collectors.toList());
            retryOrUpdateSchemaWhenAppendField(dynamicMessages);
            for (DataWriter.DataWriterRequest dataWriterRequest : batch) {
                dataWriterRequest.getRecord().ack();
                log.info("Append success, ack this message <{}>",
                        dataWriterRequest.getRecord().getMessage().get().getMessageId());
            }
            batch.clear();
            lastFlushTime = System.currentTimeMillis();
        }
    }

    private void retryOrUpdateSchemaWhenAppendField(List<DynamicMessage> dynamicMessages) {
        int retryCount = 0;
        while (true) {
            Throwable exception = null;
            try {
                dataWriter.append(dynamicMessages).get(10, TimeUnit.SECONDS);
            } catch (ExecutionException e) {
                exception = e.getCause();
            } catch (Exception e) {
                exception = e;
            }

            if (exception == null) {
                break;
            } else if (retryCount >= failedMaxRetryNum) {
                throw new BQConnectorDirectFailException(
                        String.format("Append failed try %s count still failed.", failedMaxRetryNum), exception);
            } else if (exception instanceof NotFoundException) {
                log.warn("Happen exception <{}>,retry to after update schema", exception.getMessage());
                List<Record<GenericObject>> records =
                        batch.stream().map(dataWriterRequest -> dataWriterRequest.getRecord())
                                .collect(Collectors.toList());
                schemaManager.updateSchema(records);
                updateStream(schemaManager.getProtoSchema());
                retryCount++;
            } else if (exception instanceof InternalException
                    || exception instanceof UnknownException
                    || exception instanceof UnavailableException
                    || exception instanceof ResourceExhaustedException) {
                // Exceptions that can be retried trying to recover.
                retryCount++;
                Uninterruptibles.sleepUninterruptibly(2, TimeUnit.SECONDS);
                log.warn("Happen exception <{}>, retry to after 2 seconds", exception.getMessage());
            } else {
                throw new BQConnectorDirectFailException("Encountered an unrecoverable exception, "
                        + "please check the error code for help from the documentation: "
                        + "https://cloud.google.com/bigquery/docs/error-messages", exception);
            }
        }
    }

    public void updateStream(ProtoSchema protoSchema) {
        dataWriter.updateStream(protoSchema);
    }

    public void close() throws Exception {
        dataWriter.close();
    }
}
