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
import static org.mockito.Mockito.verify;
import static org.powermock.api.mockito.PowerMockito.when;
import com.google.api.gax.grpc.GrpcStatusCode;
import com.google.api.gax.rpc.ApiException;
import com.google.api.gax.rpc.InternalException;
import com.google.api.gax.rpc.NotFoundException;
import com.google.api.gax.rpc.ResourceExhaustedException;
import com.google.api.gax.rpc.UnavailableException;
import com.google.api.gax.rpc.UnknownException;
import com.google.cloud.bigquery.storage.v1.AppendRowsResponse;
import io.grpc.Status;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.schema.GenericObject;
import org.apache.pulsar.functions.api.Record;
import org.junit.Test;
import org.mockito.Mockito;

/**
 * Data writer batch wrapper test.
 */
public class DataWriterBatchWrapperTest {

    @Test(timeout = 5000)
    @SuppressWarnings("unchecked")
    public void testAppendUpdateSchema() {

        DataWriter dataWriter = mock(DataWriter.class);

        AtomicInteger appendCount = new AtomicInteger();
        when(dataWriter.append(Mockito.any())).then(__ -> {
            CompletableFuture<AppendRowsResponse> completableFuture = new CompletableFuture<>();
            if (appendCount.get() == 0) {
                appendCount.getAndIncrement();
                completableFuture.completeExceptionally(
                        new NotFoundException(new RuntimeException(), GrpcStatusCode.of(Status.Code.NOT_FOUND), true));
            } else {
                completableFuture.complete(AppendRowsResponse.newBuilder().build());
            }
            return completableFuture;
        });

        SchemaManager schemaManager = mock(SchemaManager.class);
        DataWriterBatchWrapper dataWriterBatchWrapper = new DataWriterBatchWrapper(dataWriter, schemaManager,
                5, 10000, 2,
                10, Executors.newSingleThreadScheduledExecutor());

        Record record = new MockRecord(new CountDownLatch(0));
        for (int i = 0; i < 5; i++) {
            dataWriterBatchWrapper.append(new DataWriter.DataWriterRequest(null, record));
        }

        verify(dataWriter, Mockito.times(1)).updateStream(Mockito.any());
        verify(schemaManager, Mockito.times(1)).updateSchema(Mockito.any(List.class));
    }

    @Test(timeout = 20000)
    @SuppressWarnings("unchecked")
    public void testAppendRetry() throws InterruptedException {

        DataWriter dataWriter = mock(DataWriter.class);

        List<ApiException> mockException = new ArrayList<>();
        mockException.add(new InternalException(
                new RuntimeException(), GrpcStatusCode.of(Status.Code.INTERNAL), true));
        mockException.add(new UnknownException(
                new RuntimeException(), GrpcStatusCode.of(Status.Code.UNKNOWN), true));
        mockException.add(new UnavailableException(
                new RuntimeException(), GrpcStatusCode.of(Status.Code.UNAVAILABLE), true));
        mockException.add(new ResourceExhaustedException(
                new RuntimeException(), GrpcStatusCode.of(Status.Code.RESOURCE_EXHAUSTED), true));

        AtomicInteger appendCount = new AtomicInteger();
        when(dataWriter.append(Mockito.any())).then(__ -> {
            CompletableFuture<AppendRowsResponse> completableFuture = new CompletableFuture<>();
            if (appendCount.get() < mockException.size()) {
                completableFuture.completeExceptionally(mockException.get(appendCount.get()));
                appendCount.getAndIncrement();
            } else {
                completableFuture.complete(AppendRowsResponse.newBuilder().build());
            }
            return completableFuture;
        });

        SchemaManager schemaManager = mock(SchemaManager.class);
        DataWriterBatchWrapper dataWriterBatchWrapper = new DataWriterBatchWrapper(dataWriter, schemaManager,
                1, 10000, 2,
                10, Executors.newSingleThreadScheduledExecutor());

        CountDownLatch countDownLatch = new CountDownLatch(1);
        MockRecord mockRecord = new MockRecord(countDownLatch);
        dataWriterBatchWrapper.append(new DataWriter.DataWriterRequest(null, mockRecord));
        countDownLatch.await(30000, TimeUnit.MILLISECONDS);
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
            System.out.println("ack message");
            countDownLatch.countDown();
        }

    }
}