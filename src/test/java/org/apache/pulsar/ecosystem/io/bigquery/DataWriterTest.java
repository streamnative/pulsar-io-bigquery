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

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import com.google.api.gax.grpc.GrpcStatusCode;
import com.google.api.gax.rpc.ApiException;
import com.google.api.gax.rpc.InternalException;
import com.google.api.gax.rpc.ResourceExhaustedException;
import com.google.api.gax.rpc.UnavailableException;
import com.google.api.gax.rpc.UnknownException;
import com.google.cloud.bigquery.storage.v1.AppendRowsResponse;
import com.google.cloud.bigquery.storage.v1.BigQueryWriteClient;
import com.google.cloud.bigquery.storage.v1.CreateWriteStreamRequest;
import com.google.cloud.bigquery.storage.v1.ProtoSchema;
import io.grpc.Status;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.schema.GenericObject;
import org.apache.pulsar.functions.api.Record;
import org.apache.pulsar.io.core.SinkContext;
import org.junit.Test;
import org.mockito.Mockito;

/**
 * Data writer test.
 */
public class DataWriterTest {


    @Test
    @SuppressWarnings("unchecked")
    public void testAppendRetry() {

        SchemaManager schemaManager = mock(SchemaManager.class);
        SinkContext sinkContext = mock(SinkContext.class);

        MockDataWriterCommitted mockDataWriterCommitted = spy(
                new MockDataWriterCommitted(schemaManager, sinkContext, 100));
        mockDataWriterCommitted.append(new ArrayList<>());

        verify(schemaManager, Mockito.times(2)).updateSchema(Mockito.any(List.class));
        verify(mockDataWriterCommitted, Mockito.times(2)).updateStream(Mockito.any());
        verify(mockDataWriterCommitted, Mockito.times(1)).commit();
    }

    class MockDataWriterCommitted extends AbstractDataWriter {

        private List<ApiException> mockException = new ArrayList<>();
        private int retryCount;

        protected MockDataWriterCommitted(SchemaManager schemaManager, SinkContext sinkContext, int failedMaxRetryNum) {
            super(mock(BigQueryWriteClient.class), schemaManager, sinkContext, null, failedMaxRetryNum);
            mockException.add(new InternalException(
                    new RuntimeException(), GrpcStatusCode.of(Status.Code.INTERNAL), true));
            mockException.add(new UnknownException(
                    new RuntimeException(), GrpcStatusCode.of(Status.Code.UNKNOWN), true));
            mockException.add(new UnavailableException(
                    new RuntimeException(), GrpcStatusCode.of(Status.Code.UNAVAILABLE), true));
            mockException.add(new ResourceExhaustedException(
                    new RuntimeException(), GrpcStatusCode.of(Status.Code.RESOURCE_EXHAUSTED), true));
            // can be retry code
            mockException.add(new ResourceExhaustedException(
                    new RuntimeException(), GrpcStatusCode.of(Status.Code.NOT_FOUND), true));
            mockException.add(new ResourceExhaustedException(
                    new RuntimeException(), GrpcStatusCode.of(Status.Code.PERMISSION_DENIED), true));
        }

        @Override
        protected CompletableFuture<AppendRowsResponse> appendMsgs(List<DataWriterRequest> dataWriterRequests) {
            CompletableFuture<AppendRowsResponse> completableFuture = new CompletableFuture<>();
            if (retryCount < mockException.size()) {
                completableFuture.completeExceptionally(mockException.get(retryCount));
                retryCount++;
            } else {
                completableFuture.complete(AppendRowsResponse.newBuilder().build());
            }
            return completableFuture;
        }

        @Override
        protected void commit() {}

        @Override
        public void updateStream(ProtoSchema protoSchema) {
        }

        @Override
        protected CreateWriteStreamRequest getCreateWriteStreamRequest() {
            return null;
        }
    }

    @SuppressWarnings("unchecked")
    class MockRecord implements Record<GenericObject> {

        private CountDownLatch countDownLatch;

        private Message message;

        public MockRecord(CountDownLatch countDownLatch) {
            this.countDownLatch = countDownLatch;
            MessageId messageId = Mockito.mock(MessageId.class);
            Mockito.when(messageId.toString()).thenReturn("1:1:123");
            message = Mockito.mock(Message.class);
            Mockito.when(message.getMessageId()).thenReturn(messageId);
        }

        @Override
        public GenericObject getValue() {
            return null;
        }

        @Override
        public Optional<Message<GenericObject>> getMessage() {
            return Optional.of(message);
        }

        @Override
        public void ack() {
            System.out.println("ack message: " + countDownLatch.getCount());
            countDownLatch.countDown();
        }

    }
}
