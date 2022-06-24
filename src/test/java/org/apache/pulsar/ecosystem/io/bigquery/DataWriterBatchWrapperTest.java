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
import java.util.Arrays;
import java.util.concurrent.Executors;
import org.apache.pulsar.io.core.SinkContext;
import org.junit.Test;
import org.mockito.Mockito;

/**
 * Data writer batch wrapper test.
 */
public class DataWriterBatchWrapperTest {

    @Test
    public void testAppendMaxSize() {

        DataWriter dataWriter = mock(DataWriter.class);

        int messageNum = 5;
        DataWriterBatchWrapper dataWriterBatchWrapper = new DataWriterBatchWrapper(dataWriter, messageNum,
                10000, 20000,
                Executors.newSingleThreadScheduledExecutor(), Mockito.mock(SinkContext.class));

        for (int i = 0; i < messageNum; i++) {
            dataWriterBatchWrapper.append(Arrays.asList(new DataWriter.DataWriterRequest(null, null)));
        }

        verify(dataWriter, Mockito.times(1)).append(Mockito.any());
    }

    @Test
    public void testAppendMaxTime() throws InterruptedException {

        DataWriter dataWriter = mock(DataWriter.class);

        int messageNum = 5;
        DataWriterBatchWrapper dataWriterBatchWrapper = new DataWriterBatchWrapper(dataWriter, 10000,
                1000, 100,
                Executors.newSingleThreadScheduledExecutor(), Mockito.mock(SinkContext.class));

        for (int i = 0; i < messageNum; i++) {
            dataWriterBatchWrapper.append(Arrays.asList(new DataWriter.DataWriterRequest(null, null)));
        }

        Thread.sleep(3000);
        verify(dataWriter, Mockito.times(1)).append(Mockito.any());
    }
}