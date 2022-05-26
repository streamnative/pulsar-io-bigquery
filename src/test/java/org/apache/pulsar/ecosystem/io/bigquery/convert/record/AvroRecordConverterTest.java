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

import static org.apache.pulsar.ecosystem.io.bigquery.convert.DefaultSystemFieldConvert.MAP_KEY_NAME;
import static org.apache.pulsar.ecosystem.io.bigquery.convert.DefaultSystemFieldConvert.MAP_VALUE_NAME;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;
import com.google.cloud.bigquery.storage.v1.BigDecimalByteStringEncoder;
import com.google.cloud.bigquery.storage.v1.ProtoRows;
import com.google.cloud.bigquery.storage.v1.TableSchema;
import com.google.protobuf.ByteString;
import com.google.protobuf.Descriptors;
import com.google.protobuf.DynamicMessage;
import java.io.IOException;
import java.math.BigDecimal;
import java.util.List;
import java.util.Map;
import org.apache.pulsar.client.api.schema.GenericRecord;
import org.apache.pulsar.ecosystem.io.bigquery.AvroRecordsUtils;
import org.apache.pulsar.ecosystem.io.bigquery.BigQueryConfig;
import org.apache.pulsar.ecosystem.io.bigquery.SchemaManager;
import org.apache.pulsar.ecosystem.io.bigquery.SchemaManagerTest;
import org.apache.pulsar.ecosystem.io.bigquery.exception.RecordConvertException;
import org.apache.pulsar.functions.api.Record;
import org.junit.Assert;
import org.junit.Test;

/**
 * Avro record converter test.
 */
public class AvroRecordConverterTest {

    @Test
    public void testConvertRecord() throws IOException, RecordConvertException {

        AvroRecordConverter avroRecordConverter = new AvroRecordConverter();

        BigQueryConfig bigQueryConfig = spy(BigQueryConfig.class);
        bigQueryConfig.setProjectId("project_id");
        bigQueryConfig.setDatasetName("dateset_name");
        bigQueryConfig.setTableName("table_name");
        SchemaManagerTest.BigQueryMock bigQueryMock = new SchemaManagerTest.BigQueryMock();
        when(bigQueryConfig.createBigQuery()).thenReturn(bigQueryMock.getBigQuery());
        bigQueryConfig.setAutoCreateTable(true);
        bigQueryConfig.setAutoUpdateTable(true);
        bigQueryConfig.setClusteredTables(true);
        bigQueryConfig.setPartitionedTables(true);
        bigQueryConfig.setPartitionedTableIntervalDay(10);
        SchemaManager schemaManager = new SchemaManager(bigQueryConfig);

        Record<GenericRecord> recordRecordFirst = AvroRecordsUtils.getGenericRecordRecordFirst();
        schemaManager.createTable(recordRecordFirst);

        TableSchema firstTableSchema = schemaManager.getTableSchema();
        Descriptors.Descriptor firstDescriptor = schemaManager.getDescriptor();

        // first convert success.
        ProtoRows protoRows =
                avroRecordConverter.convertRecord(recordRecordFirst, firstDescriptor, firstTableSchema.getFieldsList());
        ByteString serializedRows = protoRows.getSerializedRows(0);
        DynamicMessage dynamicMessage = DynamicMessage.parseFrom(schemaManager.getDescriptor(), serializedRows);
        assertEquals(dynamicMessage.getAllFields().size(), 6);
        assertConvertResult(dynamicMessage);

        // second convert, miss col4 field
        Record<GenericRecord> recordRecordSecond = AvroRecordsUtils.getGenericRecordRecordSecond();
        try {
            avroRecordConverter.convertRecord(recordRecordSecond, firstDescriptor, firstTableSchema.getFieldsList());
            fail("Should has failed");
        } catch (RecordConvertException e) {
        }
    }

    @SuppressWarnings("unchecked")
    private void assertConvertResult(DynamicMessage dynamicMessage) {
        System.out.println(dynamicMessage);
        dynamicMessage.getAllFields().forEach((field, value) -> {
            String fieldName = field.getName();
            switch (fieldName) {
                case "__message_id__":
                    Assert.assertEquals("1:1:123", value);
                    break;
                case "__event_time__":
                    Assert.assertEquals(1653530376803000L, value);
                    break;
                case "col1":
                    Assert.assertEquals("test col2", value);
                    break;
                case "col3":
                    Assert.assertEquals("test col3", value);
                    break;
                case "map3":
                    assertMap3((List<DynamicMessage>) value);
                    break;
                case "tag":
                    assertTag((DynamicMessage) value);
                    break;
                default:
                    fail("To find the assertion field, add an assertion to the new field:" + fieldName);
            }
        });
    }

    private void assertTag(DynamicMessage dynamicMessage) {
        dynamicMessage.getAllFields().forEach((key, value) -> {
            String name = key.getName();
            switch (name) {
                case "booleanv":
                    assertEquals(true, value);
                    break;
                case "bytesv":
                    byte[] bytes = ((ByteString) value).toByteArray();
                    assertEquals(bytes.length, 3);
                    break;
                case "bytev":
                    assertEquals(65L, value);
                    break;
                case "charv":
                    assertEquals(97L, value);
                    break;
                case "doublev":
                    assertEquals(100.123, value);
                    break;
                case "floatv":
                    assertEquals(123.2d, (double) value, 0.00001);
                    break;
                case "inta":
                    assertEquals(1L, value);
                    break;
                case "intb":
                    assertEquals(1653530376803L, value);
                    break;
                default:
                    fail("To find the assertion field, add an assertion to the new field:" + name);

            }
        });
    }

    @SuppressWarnings("unchecked")
    private void assertMap3(List<DynamicMessage> dynamicMessages) {

        DynamicMessage mapValue = null;
        for (DynamicMessage dynamicMessage : dynamicMessages) {
            Map<Descriptors.FieldDescriptor, Object> allFields = dynamicMessage.getAllFields();
            for (Map.Entry<Descriptors.FieldDescriptor, Object> entry : allFields.entrySet()) {
                Descriptors.FieldDescriptor field = entry.getKey();
                assertTrue(field.getName() != MAP_KEY_NAME || field.getName() != MAP_VALUE_NAME);
                if (field.getName() == MAP_VALUE_NAME) {
                    mapValue = (DynamicMessage) entry.getValue();
                }

            }
        }
        if (mapValue == null) {
            fail("Map value dont is null");
        }

        mapValue.getAllFields().forEach((key, value) -> {
            String name = key.getName();
            switch (name) {
                case "test":
                    assertEquals("first data", value);
                    break;
                case "array":
                    assertEquals(((List<DynamicMessage>) value).size(), 3);
                    break;
                case "bigdecimal":
                    BigDecimal bigDecimal = BigDecimalByteStringEncoder.decodeBigNumericByteString((ByteString) value);
                    assertEquals(bigDecimal.stripTrailingZeros(), new BigDecimal("123.45"));
                    break;
                case "date":
                    assertEquals(23456, value);
                    break;
                case "localtimesmillis":
                    assertEquals(142311172665524408L, value);
                    break;
                case "timemillis":
                    assertEquals(415988644L, value);
                    break;
                case "timestampmillis":
                    assertEquals(1653530376803000L, value);
                    break;
                default:
                    fail("To find the assertion field, add an assertion to the new field:" + name);
            }
        });
    }

}