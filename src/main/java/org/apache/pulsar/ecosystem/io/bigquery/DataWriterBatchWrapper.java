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

import com.google.cloud.bigquery.storage.v1.ProtoSchema;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.io.core.SinkContext;

/**
 * Temporarily used as a wrapper for batch processing,
 * and remove it after waiting for Sink to support batch processing.
 */
@Slf4j
public class DataWriterBatchWrapper implements DataWriter{

    private final DataWriter dataWriter;
    private final int batchMaxSize;
    private final int batchMaxTime;
    // Maximum number of retries after encountering a retryable exception
    private final List<DataWriter.DataWriterRequest> batch;
    private final int batchFlushIntervalTime;
    private final ScheduledExecutorService scheduledExecutorService;
    private long lastFlushTime;
    private final SinkContext sinkContext;

    /**
     * Batch writer wrapper.
     *
     * @param dataWriter               {@link DataWriter}
     * @param batchMaxSize             Batch max size.
     * @param batchMaxTime             Batch max wait time.
     * @param batchFlushIntervalTime   Batch trigger flush interval time: milliseconds
     * @param scheduledExecutorService Schedule flush thread, this class does not guarantee thread safety,
     *                                 you need to ensure that the thread calling append is this thread.
     * @param sinkContext
     */
    public DataWriterBatchWrapper(DataWriter dataWriter, int batchMaxSize,
                                  int batchMaxTime, int batchFlushIntervalTime,
                                  ScheduledExecutorService scheduledExecutorService, SinkContext sinkContext) {
        this.dataWriter = dataWriter;
        this.batchMaxSize = batchMaxSize;
        this.batchMaxTime = batchMaxTime;
        this.batchFlushIntervalTime = batchFlushIntervalTime;
        this.scheduledExecutorService = scheduledExecutorService;
        this.batch = new ArrayList<>(batchMaxSize);
        this.sinkContext = sinkContext;
        this.lastFlushTime = System.currentTimeMillis();
        log.info("Start timed trigger refresh service, batchMaxSize:[{}], batchMaxTime:[{}] "
                + "batchFlushIntervalTime:[{}]", batchMaxSize, batchMaxTime, batchFlushIntervalTime);
        // start scheduled trigger flush timeout data.
        this.scheduledExecutorService.scheduleAtFixedRate(() -> tryFlush(),
                batchFlushIntervalTime, batchFlushIntervalTime, TimeUnit.MILLISECONDS);
    }

    @Override
    public void append(List<DataWriterRequest> dataWriterRequests) {
        sinkContext.recordMetric(MetricContent.RECEIVE_COUNT, batch.size());
        batch.addAll(dataWriterRequests);
        tryFlush();
    }

    private void tryFlush() {
        if (batch.size() > 0
                && (batch.size() >= batchMaxSize || System.currentTimeMillis() - lastFlushTime > batchMaxTime)) {
            log.info("flush size {}", batch.size());
            dataWriter.append(batch);
            batch.clear();
            lastFlushTime = System.currentTimeMillis();
        }
    }

    @Override
    public void updateStream(ProtoSchema protoSchema) {
        dataWriter.updateStream(protoSchema);
    }

    @Override
    public void close() throws Exception {
        dataWriter.close();
    }
}
