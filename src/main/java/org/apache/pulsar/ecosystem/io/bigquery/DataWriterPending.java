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
import com.google.cloud.bigquery.storage.v1.BatchCommitWriteStreamsRequest;
import com.google.cloud.bigquery.storage.v1.BatchCommitWriteStreamsResponse;
import com.google.cloud.bigquery.storage.v1.BigQueryWriteClient;
import com.google.cloud.bigquery.storage.v1.CreateWriteStreamRequest;
import com.google.cloud.bigquery.storage.v1.StorageError;
import com.google.cloud.bigquery.storage.v1.TableName;
import com.google.cloud.bigquery.storage.v1.WriteStream;
import com.google.protobuf.DynamicMessage;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.ecosystem.io.bigquery.exception.BigQueryConnectorRuntimeException;

/**
 * data writer use committed mould.
 */
@Slf4j
public class DataWriterPending extends DataWriterCommitted {


    public DataWriterPending(BigQueryWriteClient client, TableName tableName) {
        super(client, tableName);
    }

    @Override
    public CompletableFuture<AppendRowsResponse> append(List<DynamicMessage> dynamicMessages) {
        tryFetchStream(protoSchemaCache);
        CompletableFuture<AppendRowsResponse> result = super.append(dynamicMessages);
        BatchCommitWriteStreamsRequest commitRequest =
                BatchCommitWriteStreamsRequest.newBuilder()
                        .setParent(tableName.toString())
                        .addWriteStreams(writeStream.getName())
                        .build();
        closeStream();
        BatchCommitWriteStreamsResponse commitResponse = client.batchCommitWriteStreams(commitRequest);
        if (!commitResponse.hasCommitTime()) {
            List<String> errorMsg = new ArrayList<>();
            for (StorageError err : commitResponse.getStreamErrorsList()) {
                errorMsg.add(err.getErrorMessage());
            }
            result.completeExceptionally(
                    new BigQueryConnectorRuntimeException("Error committing the streams:" + errorMsg.toString()));
        }
        return result;
    }

    @Override
    protected CreateWriteStreamRequest getCreateWriteStreamRequest() {
        CreateWriteStreamRequest createWriteStreamRequest =
                CreateWriteStreamRequest.newBuilder()
                        .setParent(this.tableName.toString())
                        .setWriteStream(WriteStream.newBuilder().setType(WriteStream.Type.PENDING).build())
                        .build();
        return createWriteStreamRequest;
    }

}
