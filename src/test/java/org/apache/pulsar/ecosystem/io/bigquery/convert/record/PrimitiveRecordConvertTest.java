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
package org.apache.pulsar.ecosystem.io.bigquery.convert.record;

import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.spy;
import com.google.cloud.bigquery.storage.v1.ProtoRows;
import com.google.cloud.bigquery.storage.v1.TableSchema;
import com.google.protobuf.ByteString;
import com.google.protobuf.Descriptors;
import com.google.protobuf.DynamicMessage;
import java.io.IOException;
import java.sql.Time;
import java.sql.Timestamp;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.ToString;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.schema.GenericObject;
import org.apache.pulsar.ecosystem.io.bigquery.BigQueryConfig;
import org.apache.pulsar.ecosystem.io.bigquery.PrimitiveRecordsUtils;
import org.apache.pulsar.ecosystem.io.bigquery.SchemaManager;
import org.apache.pulsar.ecosystem.io.bigquery.SchemaManagerTest;
import org.apache.pulsar.ecosystem.io.bigquery.convert.DefaultSystemFieldConvert;
import org.apache.pulsar.ecosystem.io.bigquery.exception.RecordConvertException;
import org.apache.pulsar.functions.api.Record;
import org.junit.Assert;
import org.junit.Test;

/**
 * primitive record convert test.
 */
public class PrimitiveRecordConvertTest {

    @Test
    public void convertUserSchema() throws IOException, RecordConvertException {
        List<PrimitiveTestWrapper> allTypeField = new ArrayList<>();
        allTypeField.add(new PrimitiveTestWrapper<String>(Schema.STRING, "str", "str"));
        allTypeField.add(new PrimitiveTestWrapper<Boolean>(Schema.BOOL, true, true));
        allTypeField.add(new PrimitiveTestWrapper<Byte>(Schema.INT8,
                Integer.valueOf(100).byteValue(), 100L));
        allTypeField.add(new PrimitiveTestWrapper<Short>(Schema.INT16,
                Integer.valueOf(123).shortValue(), 123L));
        allTypeField.add(new PrimitiveTestWrapper<Integer>(Schema.INT32, 123, 123L));
        allTypeField.add(new PrimitiveTestWrapper<Long>(Schema.INT64, 123L, 123L));
        allTypeField.add(new PrimitiveTestWrapper<Float>(Schema.FLOAT, 123.1f, 123.1d));
        allTypeField.add(new PrimitiveTestWrapper<Double>(Schema.DOUBLE, 123.1d, 123.1d));
        allTypeField.add(new PrimitiveTestWrapper<byte[]>(Schema.BYTES, "aa".getBytes(), "aa".getBytes()));

        // Assertion value generated from CivilTimeEncoder
        allTypeField.add(new PrimitiveTestWrapper<Date>(Schema.DATE,
                                                       new Date(1654849667447L), 1654849667447000L));
        allTypeField.add(new PrimitiveTestWrapper<Time>(Schema.TIME,
                                                        new Time(1654849667447L), 1654849667447000L));
        allTypeField.add(new PrimitiveTestWrapper<Timestamp>(Schema.TIMESTAMP,
                                                        new Timestamp(1654849667447L), 1654849667447000L));
        allTypeField.add(new PrimitiveTestWrapper<Instant>(Schema.INSTANT, Instant.ofEpochMilli(1654826254091L),
                                                 142313371551753080L));
        LocalDate localDate = LocalDate.now();
        allTypeField.add(new PrimitiveTestWrapper<LocalDate>(Schema.LOCAL_DATE,
                                                             localDate, (int) localDate.toEpochDay()));
        LocalTime localTime = LocalTime.ofNanoOfDay(59586665000000L);
        allTypeField.add(new PrimitiveTestWrapper<LocalTime>(Schema.LOCAL_TIME, localTime, 70941025704L));
        LocalDateTime localDateTime = LocalDateTime.of(1997, 9, 13, 20,
                                                      30, 59, 125000);
        allTypeField.add(new PrimitiveTestWrapper<LocalDateTime>(Schema.LOCAL_DATE_TIME, localDateTime,
                140567839222268029L));

        PrimitiveRecordConvert primitiveRecordConvert = new PrimitiveRecordConvert();
        for (PrimitiveTestWrapper primitiveTestWrapper : allTypeField) {
            BigQueryConfig bigQueryConfig = spy(BigQueryConfig.class);
            bigQueryConfig.setProjectId("project_id");
            bigQueryConfig.setDatasetName("dateset_name");
            bigQueryConfig.setTableName("table_name");
            bigQueryConfig.setAutoCreateTable(true);
            bigQueryConfig.setAutoUpdateTable(true);
            bigQueryConfig.setClusteredTables(true);
            bigQueryConfig.setPartitionedTables(true);
            bigQueryConfig.setPartitionedTableIntervalDay(10);
            SchemaManagerTest.BigQueryMock bigQueryMock = new SchemaManagerTest.BigQueryMock();
            doReturn(bigQueryMock.getBigQuery()).when(bigQueryConfig).createBigQuery();
            SchemaManager schemaManager = new SchemaManager(bigQueryConfig);
            Record<GenericObject> record = PrimitiveRecordsUtils.getRecord(primitiveTestWrapper.getType(),
                    primitiveTestWrapper.getValue());
            schemaManager.initTable(record);
            TableSchema tableSchema = schemaManager.getTableSchema();
            Descriptors.Descriptor descriptor = schemaManager.getDescriptor();
            System.out.println("test :" + primitiveTestWrapper);
            ProtoRows protoRows = primitiveRecordConvert.convertRecord(record, descriptor, tableSchema.getFieldsList());
            ByteString serializedRows = protoRows.getSerializedRows(0);
            DynamicMessage dynamicMessage = DynamicMessage.parseFrom(schemaManager.getDescriptor(), serializedRows);
            dynamicMessage.getAllFields().forEach((fieldDescriptor, o) -> {
                if (fieldDescriptor.getName().equals(DefaultSystemFieldConvert.PRIMITIVE_VALUE_NAME)) {
                    primitiveTestWrapper.assertEquals(o);
                }
            });
        }
    }

    @Data
    @ToString
    @AllArgsConstructor
    class PrimitiveTestWrapper<T> {
        private Schema<T> type;
        private Object value;
        private Object assertValue;

        private void assertEquals(Object recordValue) {
            if (recordValue instanceof Double) {
                Assert.assertEquals((double) recordValue, (double) assertValue, 0.001);
            } else if (recordValue instanceof ByteString) {
                ByteString byteStr = (ByteString) recordValue;
                Assert.assertArrayEquals(byteStr.toByteArray(), (byte[]) assertValue);
            } else {
                Assert.assertEquals(recordValue, assertValue);
            }
        }
    }
}
