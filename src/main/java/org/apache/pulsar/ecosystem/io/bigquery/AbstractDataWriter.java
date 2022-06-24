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
import com.google.cloud.bigquery.storage.v1.AppendRowsResponse;
import com.google.cloud.bigquery.storage.v1.BigQueryWriteClient;
import com.google.cloud.bigquery.storage.v1.CreateWriteStreamRequest;
import com.google.cloud.bigquery.storage.v1.FinalizeWriteStreamRequest;
import com.google.cloud.bigquery.storage.v1.ProtoSchema;
import com.google.cloud.bigquery.storage.v1.StreamWriter;
import com.google.cloud.bigquery.storage.v1.TableName;
import com.google.cloud.bigquery.storage.v1.WriteStream;
import com.google.common.util.concurrent.Uninterruptibles;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.client.api.schema.GenericObject;
import org.apache.pulsar.ecosystem.io.bigquery.exception.BQConnectorDirectFailException;
import org.apache.pulsar.functions.api.Record;
import org.apache.pulsar.io.core.SinkContext;

/**
 * abstract data writer.
 */
@Slf4j
public abstract class AbstractDataWriter implements DataWriter {

    // Maximum number of retries after encountering a retryable exception
    private final int failedMaxRetryNum;
    private final SchemaManager schemaManager;
    private final SinkContext sinkContext;

    protected final BigQueryWriteClient client;
    protected final TableName tableName;
    protected WriteStream writeStream;
    protected StreamWriter streamWriter;
    // cache the latest table schema
    protected volatile ProtoSchema protoSchemaCache;

    protected AbstractDataWriter(BigQueryWriteClient client, SchemaManager schemaManager, SinkContext sinkContext,
                                 TableName tableName, int failedMaxRetryNum) {
        this.failedMaxRetryNum = failedMaxRetryNum;
        this.schemaManager = schemaManager;
        this.sinkContext = sinkContext;
        this.client = client;
        this.tableName = tableName;
    }

    protected abstract CompletableFuture<AppendRowsResponse> appendMsgs(List<DataWriterRequest> dataWriterRequests);

    protected abstract void commit();

    protected abstract CreateWriteStreamRequest getCreateWriteStreamRequest();

    @Override
    public void append(List<DataWriterRequest> dataWriterRequests) {
        int retryCount = 0;
        while (true) {
            Throwable exception = null;
            try {
                appendMsgs(dataWriterRequests).get(10, TimeUnit.SECONDS);
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
                                dataWriterRequests.stream().map(dataWriterRequest -> dataWriterRequest.getRecord())
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
        commit();
    }

    @Override
    public void updateStream(ProtoSchema protoSchema) {
        try {
            closeStream();
        } catch (Exception e) {
            log.warn("Close stream exception, ignore. {} ", e.getMessage());
        }
        // Wait a while before trying to update the stream
        Uninterruptibles.sleepUninterruptibly(5, TimeUnit.SECONDS);
        tryFetchStream(protoSchema);
    }


    @Override
    public void close() {
        closeStream();
        client.close();
    }

    /**
     * ack message.
     * @param dataWriterRequests
     */
    protected void ack(List<DataWriterRequest> dataWriterRequests) {
        for (DataWriter.DataWriterRequest dataWriterRequest : dataWriterRequests) {
            dataWriterRequest.getRecord().ack();
            log.info("Append success, ack this message <{}>",
                    dataWriterRequest.getRecord().getMessage().get().getMessageId());
            sinkContext.recordMetric(MetricContent.ACK_COUNT, 1);
        }
    }

    protected void tryFetchStream(ProtoSchema protoSchema) {
        this.protoSchemaCache = protoSchema;
        int tryCount = 0;
        while (true) {
            try {
                CreateWriteStreamRequest createWriteStreamRequest = getCreateWriteStreamRequest();
                writeStream = client.createWriteStream(createWriteStreamRequest);
                streamWriter = StreamWriter
                        .newBuilder(writeStream.getName(), client)
                        .setWriterSchema(protoSchema)
                        .build();
                log.info("Update resources success, start new write stream: {}", writeStream.getName());
                return;
            } catch (Exception e) {
                if (tryCount >= 5) {
                    throw new BQConnectorDirectFailException(
                            "Update big query resources failed, it doesn't work even after 5 tries, please check", e);
                } else {
                    tryCount++;
                    log.warn("Update big query resources count <{}> failed, Retry after 5 seconds <{}>",
                            tryCount, e.getMessage());
                    Uninterruptibles.sleepUninterruptibly(5, TimeUnit.SECONDS);
                }
            }
        }
    }

    protected void closeStream() {
        if (streamWriter != null) {
            streamWriter.close();
            FinalizeWriteStreamRequest finalizeWriteStreamRequest =
                    FinalizeWriteStreamRequest.newBuilder().setName(writeStream.getName()).build();
            client.finalizeWriteStream(finalizeWriteStreamRequest);
        }
    }
}
