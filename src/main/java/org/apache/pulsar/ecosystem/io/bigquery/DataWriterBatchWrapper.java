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

import com.google.api.gax.rpc.ApiException;
import com.google.api.gax.rpc.StatusCode;
import com.google.cloud.bigquery.storage.v1.ProtoSchema;
import com.google.common.util.concurrent.Uninterruptibles;
import com.google.protobuf.DynamicMessage;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
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
import org.apache.pulsar.io.core.SinkContext;

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
    private final SinkContext sinkContext;


    /**
     * Batch writer wrapper.
     *
     * @param dataWriter               {@link DataWriter}
     * @param schemaManager            {@link SchemaManager}
     * @param batchMaxSize             Batch max size.
     * @param batchMaxTime             Batch max wait time.
     * @param batchFlushIntervalTime   Batch trigger flush interval time: milliseconds
     * @param failedMaxRetryNum        When append failed, max retry num.
     * @param scheduledExecutorService Schedule flush thread, this class does not guarantee thread safety,
     *                                 you need to ensure that the thread calling append is this thread.
     * @param sinkContext
     */
    public DataWriterBatchWrapper(DataWriter dataWriter, SchemaManager schemaManager,
                                  int batchMaxSize, int batchMaxTime, int batchFlushIntervalTime,
                                  int failedMaxRetryNum, ScheduledExecutorService scheduledExecutorService,
                                  SinkContext sinkContext) {
        this.dataWriter = dataWriter;
        this.schemaManager = schemaManager;
        this.batchMaxSize = batchMaxSize;
        this.batchMaxTime = batchMaxTime;
        this.batchFlushIntervalTime = batchFlushIntervalTime;
        this.failedMaxRetryNum = failedMaxRetryNum;
        this.scheduledExecutorService = scheduledExecutorService;
        this.batch = new ArrayList<>(batchMaxSize);
        this.sinkContext = sinkContext;
    }

    /**
     * start scheduled trigger flush timeout data.
     */
    public void init() {
        this.lastFlushTime = System.currentTimeMillis();
        log.info("Start timed trigger refresh service, batchMaxSize:[{}], "
                        + "batchMaxTime:[{}] batchFlushIntervalTime:[{}] failedMaxRetryNum:[{}]",
                batchMaxSize, batchMaxTime, batchFlushIntervalTime, failedMaxRetryNum);
        this.scheduledExecutorService.scheduleAtFixedRate(() -> tryFlush(),
                batchFlushIntervalTime, batchFlushIntervalTime, TimeUnit.MILLISECONDS);
    }

    public void append(DataWriter.DataWriterRequest dataWriterRequests) {
        sinkContext.recordMetric(MetricContent.RECEIVE_COUNT, batch.size());
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
                sinkContext.recordMetric(MetricContent.ACK_COUNT, 1);
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
                sinkContext.recordMetric(MetricContent.FAIL_COUNT, 1);
            } catch (Exception e) {
                exception = e;
                sinkContext.recordMetric(MetricContent.FAIL_COUNT, 1);
            }

            // Merge two exception states
            Status.Code errorCode = null;
            if (exception instanceof StatusRuntimeException) {
                StatusRuntimeException statusRuntimeException = (StatusRuntimeException) exception;
                errorCode = statusRuntimeException.getStatus().getCode();
            }
            if (exception instanceof ApiException) {
                ApiException apiException = (ApiException) exception;
                StatusCode statusCode = apiException.getStatusCode();
                errorCode = (Status.Code) statusCode.getTransportCode();
            }

            if (exception == null) {
                break;
            } else if (retryCount >= failedMaxRetryNum) {
                throw new BQConnectorDirectFailException(
                        String.format("Append failed try %s count still failed.", failedMaxRetryNum), exception);
            } else if (errorCode != null) {
                switch (errorCode) {
                    case NOT_FOUND:
                    case PERMISSION_DENIED:
                        log.warn("RetryCount [{}] Happen exception <{}>,retry to after update schema",
                                retryCount, exception.getMessage());
                        List<Record<GenericObject>> records =
                                batch.stream().map(dataWriterRequest -> dataWriterRequest.getRecord())
                                        .collect(Collectors.toList());
                        schemaManager.updateSchema(records);
                        updateStream(schemaManager.getProtoSchema());
                        retryCount++;
                        sinkContext.recordMetric(MetricContent.UPDATE_SCHEMA_COUNT, 1);
                        break;
                    case INTERNAL:
                    case UNKNOWN:
                    case UNAVAILABLE:
                    case ABORTED:
                    case RESOURCE_EXHAUSTED:
                        // Exceptions that can be retried trying to recover.
                        retryCount++;
                        Uninterruptibles.sleepUninterruptibly(2, TimeUnit.SECONDS);
                        log.warn("RetryCount [{}] Happen exception <{}>, retry to after 2 seconds",
                                retryCount, exception.getMessage());
                        break;
                    default:
                        throw new BQConnectorDirectFailException("Encountered an unrecoverable exception, "
                                + "please check the error code for help from the documentation: "
                                + "https://cloud.google.com/bigquery/docs/error-messages", exception);
                }
            } else {
                throw new BQConnectorDirectFailException("Encountered an unrecoverable exception, "
                        + "please check the error code for help from the documentation: "
                        + "https://cloud.google.com/bigquery/docs/error-messages", exception);
            }
        }
        for (int i = 0; i < retryCount; i++) {
            sinkContext.recordMetric(MetricContent.RETRY_COUNT, 1);
        }
    }

    public void updateStream(ProtoSchema protoSchema) {
        dataWriter.updateStream(protoSchema);
    }

    public void close() throws Exception {
        dataWriter.close();
    }
}
