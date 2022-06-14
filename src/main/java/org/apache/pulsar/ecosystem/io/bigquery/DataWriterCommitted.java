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
import com.google.cloud.bigquery.storage.v1.FinalizeWriteStreamRequest;
import com.google.cloud.bigquery.storage.v1.ProtoRows;
import com.google.cloud.bigquery.storage.v1.ProtoSchema;
import com.google.cloud.bigquery.storage.v1.StreamWriter;
import com.google.cloud.bigquery.storage.v1.TableName;
import com.google.cloud.bigquery.storage.v1.WriteStream;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.common.util.concurrent.Uninterruptibles;
import com.google.protobuf.DynamicMessage;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.ecosystem.io.bigquery.exception.BigQueryConnectorRuntimeException;

/**
 * data writer use committed mould.
 */
@Slf4j
public class DataWriterCommitted implements DataWriter {

    protected final BigQueryWriteClient client;
    protected final TableName tableName;
    protected WriteStream writeStream;
    protected StreamWriter streamWriter;
    // cache the latest table schema
    protected volatile ProtoSchema protoSchemaCache;

    public DataWriterCommitted(BigQueryWriteClient client, TableName tableName) {
        this.client = client;
        this.tableName = tableName;

    }

    @Override
    public CompletableFuture<AppendRowsResponse> append(List<DynamicMessage> dynamicMessages) {

        // 1. Write message
        ProtoRows.Builder rowsBuilder = ProtoRows.newBuilder();
        for (DynamicMessage dynamicMessage : dynamicMessages) {
            rowsBuilder.addSerializedRows(dynamicMessage.toByteString());
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
    public void updateStream(ProtoSchema protoSchema) {
        closeStream();
        Uninterruptibles.sleepUninterruptibly(5, TimeUnit.SECONDS);
        tryFetchStream(protoSchema);
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
                if (tryCount == 5) {
                    throw new BigQueryConnectorRuntimeException(
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

    protected CreateWriteStreamRequest getCreateWriteStreamRequest() {
        CreateWriteStreamRequest createWriteStreamRequest =
                CreateWriteStreamRequest.newBuilder()
                        .setParent(this.tableName.toString())
                        .setWriteStream(WriteStream.newBuilder().setType(WriteStream.Type.COMMITTED).build())
                        .build();
        return createWriteStreamRequest;
    }

    protected void closeStream() {
        if (streamWriter != null) {
            streamWriter.close();
            FinalizeWriteStreamRequest finalizeWriteStreamRequest =
                    FinalizeWriteStreamRequest.newBuilder().setName(writeStream.getName()).build();
            client.finalizeWriteStream(finalizeWriteStreamRequest);
        }
    }

    @Override
    public void close() {
        closeStream();
        client.close();
    }
}
