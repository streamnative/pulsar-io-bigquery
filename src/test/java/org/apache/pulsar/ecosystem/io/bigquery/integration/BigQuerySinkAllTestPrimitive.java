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
package org.apache.pulsar.ecosystem.io.bigquery.integration;

import java.sql.Time;
import java.sql.Timestamp;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.ToString;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.schema.GenericObject;
import org.apache.pulsar.ecosystem.io.bigquery.BigQuerySink;
import org.apache.pulsar.ecosystem.io.bigquery.PrimitiveRecordsUtils;
import org.apache.pulsar.functions.api.Record;

/**
 * Big query all test. Just use the local debug test.
 */
public class BigQuerySinkAllTestPrimitive {

    public static void main(String[] args) throws Exception {
        BigQuerySinkAllTestPrimitive bigQuerySinkAllTestPrimitive = new BigQuerySinkAllTestPrimitive();
        bigQuerySinkAllTestPrimitive.testBase();
    }

    public void testBase() throws Exception {
        // all test to bigquery
        List<PrimitiveTestWrapper> allTypeField = new ArrayList<>();
        allTypeField.add(new PrimitiveTestWrapper<String>(Schema.STRING, "str", "str"));
        allTypeField.add(new PrimitiveTestWrapper<Boolean>(Schema.BOOL, true, true));
        allTypeField.add(new PrimitiveTestWrapper<Byte>(Schema.INT8,
                                                        Integer.valueOf(100).byteValue(), 100));
        allTypeField.add(new PrimitiveTestWrapper<Short>(Schema.INT16,
                                                        Integer.valueOf(123).shortValue(), 123));
        allTypeField.add(new PrimitiveTestWrapper<Integer>(Schema.INT32, 123, 123));
        allTypeField.add(new PrimitiveTestWrapper<Long>(Schema.INT64, 123L, 123L));
        allTypeField.add(new PrimitiveTestWrapper<Float>(Schema.FLOAT, 123.1f, 123.1f));
        allTypeField.add(new PrimitiveTestWrapper<Double>(Schema.DOUBLE, 123.1d, 123.1d));
        allTypeField.add(new PrimitiveTestWrapper<byte[]>(Schema.BYTES, "aa".getBytes(), "aa".getBytes()));
        Date date = new Date();
        allTypeField.add(new PrimitiveTestWrapper<Date>(Schema.DATE, date, date.getTime()));
        long time = System.currentTimeMillis();
        allTypeField.add(new PrimitiveTestWrapper<Time>(Schema.TIME, new Time(time), time));
        allTypeField.add(new PrimitiveTestWrapper<Timestamp>(Schema.TIMESTAMP, new Timestamp(time), time));
        Instant instant = Instant.now();
        allTypeField.add(new PrimitiveTestWrapper<Instant>(Schema.INSTANT, instant, instant.toEpochMilli()));
        LocalDate localDate = LocalDate.now();
        allTypeField.add(new PrimitiveTestWrapper<LocalDate>(Schema.LOCAL_DATE, localDate, localDate.toEpochDay()));
        LocalTime localTime = LocalTime.ofNanoOfDay(59586665000000L);
        allTypeField.add(new PrimitiveTestWrapper<LocalTime>(Schema.LOCAL_TIME, localTime, 70941025704L));
        LocalDateTime localDateTime = LocalDateTime.now();
        allTypeField.add(new PrimitiveTestWrapper<LocalDateTime>(Schema.LOCAL_DATE_TIME, localDateTime,
                localDateTime.toInstant(ZoneOffset.UTC).toEpochMilli()));

        Map<String, Object> sinkConfig = new HashMap<>();
        sinkConfig.put("projectId", "affable-ray-226821");
        sinkConfig.put("datasetName", "testdset");
        sinkConfig.put("tableName", "primitive_date");
        sinkConfig.put("autoCreateTable", true);
        sinkConfig.put("autoUpdateTable", true);
        sinkConfig.put("clusteredTables", true);
        sinkConfig.put("partitionedTables", true);
        sinkConfig.put("partitionedTableIntervalDay", 10);
        sinkConfig.put("defaultSystemField", "__event_time__");
        BigQuerySink bigQuerySink = new BigQuerySink();
        bigQuerySink.open(sinkConfig, null);

        for (PrimitiveTestWrapper primitiveTestWrapper : allTypeField) {
            Record<GenericObject> record = PrimitiveRecordsUtils.getRecord(primitiveTestWrapper.getType(),
                    primitiveTestWrapper.getValue());
            bigQuerySink.write(record);
        }
        bigQuerySink.close();
    }

    @Data
    @ToString
    @AllArgsConstructor
    class PrimitiveTestWrapper<T> {
        private Schema<T> type;
        private Object value;
        private Object assertValue;
    }

}
