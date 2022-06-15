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

import com.google.cloud.bigquery.storage.v1.AppendRowsResponse;
import com.google.cloud.bigquery.storage.v1.ProtoSchema;
import com.google.protobuf.DynamicMessage;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.client.api.schema.GenericObject;
import org.apache.pulsar.ecosystem.io.bigquery.exception.BigQueryConnectorRuntimeException;
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
    private List<DataWriter.DataWriterRequest> batch;
    private long lastFlushTime;
    private final SchemaManager schemaManager;
    private final Executor callBackThread;

    public DataWriterBatchWrapper(DataWriter dataWriter, SchemaManager schemaManager,
                                  Executor executor, int batchMaxSize, int batchMaxTime) {
        this.dataWriter = dataWriter;
        this.schemaManager = schemaManager;
        this.callBackThread = executor;
        this.batchMaxSize = batchMaxSize;
        this.batchMaxTime = batchMaxTime;
        this.lastFlushTime = System.currentTimeMillis();
        this.batch = new ArrayList<>(batchMaxSize);
    }

    public void append(List<DataWriter.DataWriterRequest> dataWriterRequests) {
        if (dataWriterRequests != null) {
            batch.addAll(dataWriterRequests);
        }
        long currentTimeMillis = System.currentTimeMillis();
        if (batch.size() > 1 && (currentTimeMillis - lastFlushTime > batchMaxTime || batch.size() >= batchMaxSize)) {

            log.info("flush size {}", batch.size());
            List<DynamicMessage> dynamicMessages = batch.stream()
                    .map(dataWriterRequest -> dataWriterRequest.getDynamicMessage())
                    .collect(Collectors.toList());
            // 1. try first append.
            try {
                CompletableFuture<AppendRowsResponse> append = dataWriter.append(dynamicMessages);
                callBack(batch, append);
                return;
            } catch (Exception e) {
                // TODO Refinement exceptions, other exceptions, throw exceptions directly
                log.warn("Append record field, try update schema: <{}>", e.getMessage());
                List<Record<GenericObject>> records =
                        batch.stream().map(dataWriterRequest -> dataWriterRequest.getRecord())
                                .collect(Collectors.toList());
                schemaManager.updateSchema(records);
            }

            // 2. try to append again.
            try {
                updateResources(schemaManager.getProtoSchema());
                CompletableFuture<AppendRowsResponse> append = dataWriter.append(dynamicMessages);
                callBack(batch, append);
            } catch (Exception e) {
                // TODO Refinement exceptions, other exceptions, throw exceptions directly
                throw new BigQueryConnectorRuntimeException(
                        "Append record failed, after trying to update the schema it still fails", e);
            }
        }
    }

    private void callBack(List<DataWriter.DataWriterRequest> dataWriterRequests,
                          CompletableFuture<AppendRowsResponse> append) {
        append.handleAsync((appendRowsResponse, throwable) -> {
            if (throwable != null) {
                for (DataWriter.DataWriterRequest dataWriterRequest : dataWriterRequests) {
                    // TODO Refinement exceptions, other exceptions, throw exceptions directly
                    dataWriterRequest.getRecord().fail();
                    log.warn("Append fail, fail this message <{}>",
                            dataWriterRequest.getRecord().getMessage().get().getMessageId(), throwable);
                }
            } else {
                for (DataWriter.DataWriterRequest dataWriterRequest : dataWriterRequests) {
                    dataWriterRequest.getRecord().ack();
                    log.info("Append success, ack this message <{}>",
                            dataWriterRequest.getRecord().getMessage().get().getMessageId());
                }
            }
            return null;
        }, callBackThread);
        batch = new ArrayList<>(batchMaxSize);
        lastFlushTime = System.currentTimeMillis();
    }

    public void updateResources(ProtoSchema protoSchema) throws IOException {
        dataWriter.updateStream(protoSchema);
    }

    public void close() throws Exception {
        dataWriter.close();
    }
}
