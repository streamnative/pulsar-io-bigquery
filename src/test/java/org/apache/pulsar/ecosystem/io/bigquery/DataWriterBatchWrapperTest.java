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

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.powermock.api.mockito.PowerMockito.when;
import com.google.cloud.bigquery.storage.v1.AppendRowsResponse;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.pulsar.ecosystem.io.bigquery.exception.BigQueryConnectorRuntimeException;
import org.junit.Test;
import org.mockito.Mockito;

/**
 * Data writer batch wrapper test.
 */
public class DataWriterBatchWrapperTest {

    @Test
    @SuppressWarnings("unchecked")
    public void testAppend() throws Exception {

        DataWriter dataWriter = mock(DataWriter.class);

        AtomicInteger appendCount = new AtomicInteger();
        CompletableFuture<AppendRowsResponse> completableFuture = new CompletableFuture<>();
        AppendRowsResponse appendRowsResponse = AppendRowsResponse.newBuilder().build();
        when(dataWriter.append(Mockito.any())).then(__ -> {
            if (appendCount.get() == 0) {
                appendCount.getAndIncrement();
                throw new BigQueryConnectorRuntimeException("mock exception");
            } else {
                completableFuture.complete(AppendRowsResponse.newBuilder().build());
            }
            return completableFuture;
        });

        SchemaManager schemaManager = mock(SchemaManager.class);
        DataWriterBatchWrapper dataWriterBatchWrapper = new DataWriterBatchWrapper(dataWriter, schemaManager,
                Executors.newSingleThreadExecutor(), 5, 10000);

        List<DataWriter.DataWriterRequest> dataWriterRequests = new ArrayList<>();
        dataWriterRequests.add(new DataWriter.DataWriterRequest());
        dataWriterRequests.add(new DataWriter.DataWriterRequest());
        dataWriterRequests.add(new DataWriter.DataWriterRequest());
        dataWriterRequests.add(new DataWriter.DataWriterRequest());

        for (int i = 0; i < 5; i++) {
            dataWriterBatchWrapper.append(Arrays.asList(new DataWriter.DataWriterRequest()));
        }

        verify(dataWriter, Mockito.times(1)).updateStream(Mockito.any());
        verify(schemaManager, Mockito.times(1)).updateSchema(Mockito.any(List.class));
        assertEquals(appendRowsResponse, completableFuture.get(1000, TimeUnit.MILLISECONDS));
    }
}