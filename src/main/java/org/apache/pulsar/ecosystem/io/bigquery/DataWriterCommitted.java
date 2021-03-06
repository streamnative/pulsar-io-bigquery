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

import com.google.api.core.ApiFuture;
import com.google.api.core.ApiFutureCallback;
import com.google.api.core.ApiFutures;
import com.google.cloud.bigquery.storage.v1.AppendRowsResponse;
import com.google.cloud.bigquery.storage.v1.BigQueryWriteClient;
import com.google.cloud.bigquery.storage.v1.CreateWriteStreamRequest;
import com.google.cloud.bigquery.storage.v1.ProtoRows;
import com.google.cloud.bigquery.storage.v1.TableName;
import com.google.cloud.bigquery.storage.v1.WriteStream;
import com.google.common.util.concurrent.MoreExecutors;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.io.core.SinkContext;

/**
 * data writer use committed mould.
 */
@Slf4j
public class DataWriterCommitted extends AbstractDataWriter {

    private List<DataWriterRequest> waitAckMessage;

    protected DataWriterCommitted(BigQueryWriteClient client, SchemaManager schemaManager,
                                  SinkContext sinkContext, TableName tableName,
                                  int failedMaxRetryNum) {
        super(client, schemaManager, sinkContext, tableName, failedMaxRetryNum);
        this.waitAckMessage = new ArrayList<>();
    }

    @Override
    public CompletableFuture<AppendRowsResponse> appendMsgs(List<DataWriterRequest> dataWriterRequests) {
        // 1. Write message
        ProtoRows.Builder rowsBuilder = ProtoRows.newBuilder();
        for (DataWriterRequest dataWriterRequest : dataWriterRequests) {
            waitAckMessage.add(dataWriterRequest);
            rowsBuilder.addSerializedRows(dataWriterRequest.getDynamicMessage().toByteString());
        }

        // 2. append
        ApiFuture<AppendRowsResponse> append = streamWriter.append(rowsBuilder.build());
        CompletableFuture<AppendRowsResponse> result = new CompletableFuture<>();
        ApiFutures.addCallback(
                append, new ApiFutureCallback<AppendRowsResponse>() {
                    @Override
                    public void onFailure(Throwable throwable) {
                        result.completeExceptionally(throwable);
                    }

                    @Override
                    public void onSuccess(AppendRowsResponse appendRowsResponse) {
                        result.complete(appendRowsResponse);
                    }
                }, MoreExecutors.directExecutor());
        return result;
    }

    @Override
    protected CreateWriteStreamRequest getCreateWriteStreamRequest() {
        CreateWriteStreamRequest createWriteStreamRequest =
                CreateWriteStreamRequest.newBuilder()
                        .setParent(this.tableName.toString())
                        .setWriteStream(WriteStream.newBuilder().setType(WriteStream.Type.COMMITTED).build())
                        .build();
        return createWriteStreamRequest;
    }

    @Override
    protected void commit() {
        ack(waitAckMessage);
        waitAckMessage.clear();
    }
}
