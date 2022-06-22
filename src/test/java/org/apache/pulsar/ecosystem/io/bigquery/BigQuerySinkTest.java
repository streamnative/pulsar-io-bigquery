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

import static org.mockito.Mockito.times;
import com.google.cloud.bigquery.storage.v1.TableSchema;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.pulsar.ecosystem.io.bigquery.convert.record.RecordConverter;
import org.apache.pulsar.ecosystem.io.bigquery.exception.BQConnectorRecordConvertException;
import org.apache.pulsar.functions.api.Record;
import org.apache.pulsar.io.core.SinkContext;
import org.junit.Test;
import org.mockito.Mockito;

/**
 * big query sink test.
 */
public class BigQuerySinkTest {

    @Test
    @SuppressWarnings("unchecked")
    public void testWrite() throws Exception {

        DataWriterBatchWrapper dataWriterBatchWrapper = Mockito.mock(DataWriterBatchWrapper.class);

        AtomicInteger count = new AtomicInteger();
        RecordConverter recordConverter = Mockito.mock(RecordConverter.class);
        Mockito.when(recordConverter.convertRecord(Mockito.any(), Mockito.any(), Mockito.any())).thenAnswer(
                invocationOnMock -> {
                    if (count.get() == 0) {
                        count.incrementAndGet();
                        throw new BQConnectorRecordConvertException("mock record exception");
                    }
                    return null;
                });
        SchemaManager schemaManager = Mockito.mock(SchemaManager.class);
        Mockito.when(schemaManager.getDescriptor()).thenReturn(null);
        Mockito.when(schemaManager.getTableSchema()).thenReturn(TableSchema.newBuilder().build());
        SinkContext sinkContext = Mockito.mock(SinkContext.class);

        BigQueryConfig bigQueryConfig = new BigQueryConfig();
        Record record = Mockito.mock(Record.class);

        BigQuerySink bigQuerySink =
                new BigQuerySink(dataWriterBatchWrapper, recordConverter, bigQueryConfig, schemaManager, sinkContext);
        bigQuerySink.write(record);

        Mockito.verify(schemaManager, times(1)).initTable(record);
        Mockito.verify(schemaManager, times(1)).updateSchema(record);
        Mockito.verify(dataWriterBatchWrapper, times(1)).init();
        Mockito.verify(dataWriterBatchWrapper, times(2)).updateStream(Mockito.any());
        Mockito.verify(dataWriterBatchWrapper, times(1)).append(Mockito.any());
        Mockito.verify(recordConverter, times(2)).convertRecord(Mockito.any(),
                Mockito.any(), Mockito.any());

    }
}
