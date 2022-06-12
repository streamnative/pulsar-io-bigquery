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
package org.apache.pulsar.ecosystem.io.bigquery.convert.schema;

import com.google.cloud.bigquery.Field;
import com.google.cloud.bigquery.LegacySQLTypeName;
import com.google.common.collect.Sets;
import java.util.ArrayList;
import java.util.List;
import lombok.AllArgsConstructor;
import lombok.Data;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.schema.GenericObject;
import org.apache.pulsar.ecosystem.io.bigquery.PrimitiveRecordsUtils;
import org.apache.pulsar.ecosystem.io.bigquery.convert.DefaultSystemFieldConvert;
import org.apache.pulsar.functions.api.Record;
import org.junit.Assert;
import org.junit.Test;

/**
 * avro schema convert test.
 */
public class PrimitiveSchemaConvertTest {

    @Test
    public void convertUserSchema() {

        PrimitiveSchemaConvert primitiveSchemaConvert =
                new PrimitiveSchemaConvert(Sets.newHashSet("__event_time__", "__message_id__", "abc"));
        List<PrimitiveTestWrapper> allTypeField = new ArrayList<>();
        allTypeField.add(new PrimitiveTestWrapper(Schema.STRING, LegacySQLTypeName.STRING));
        allTypeField.add(new PrimitiveTestWrapper(Schema.BOOL, LegacySQLTypeName.BOOLEAN));
        allTypeField.add(new PrimitiveTestWrapper(Schema.INT8, LegacySQLTypeName.INTEGER));
        allTypeField.add(new PrimitiveTestWrapper(Schema.INT16, LegacySQLTypeName.INTEGER));
        allTypeField.add(new PrimitiveTestWrapper(Schema.INT32, LegacySQLTypeName.INTEGER));
        allTypeField.add(new PrimitiveTestWrapper(Schema.INT64, LegacySQLTypeName.INTEGER));
        allTypeField.add(new PrimitiveTestWrapper(Schema.FLOAT, LegacySQLTypeName.FLOAT));
        allTypeField.add(new PrimitiveTestWrapper(Schema.DOUBLE, LegacySQLTypeName.FLOAT));
        allTypeField.add(new PrimitiveTestWrapper(Schema.BYTES, LegacySQLTypeName.BYTES));
        allTypeField.add(new PrimitiveTestWrapper(Schema.DATE, LegacySQLTypeName.TIMESTAMP));
        allTypeField.add(new PrimitiveTestWrapper(Schema.TIME, LegacySQLTypeName.TIMESTAMP));
        allTypeField.add(new PrimitiveTestWrapper(Schema.TIMESTAMP, LegacySQLTypeName.TIMESTAMP));
        allTypeField.add(new PrimitiveTestWrapper(Schema.INSTANT, LegacySQLTypeName.DATETIME));
        allTypeField.add(new PrimitiveTestWrapper(Schema.LOCAL_DATE, LegacySQLTypeName.DATE));
        allTypeField.add(new PrimitiveTestWrapper(Schema.LOCAL_TIME, LegacySQLTypeName.TIME));
        allTypeField.add(new PrimitiveTestWrapper(Schema.LOCAL_DATE_TIME, LegacySQLTypeName.DATETIME));
        for (PrimitiveTestWrapper primitiveTestWrapper : allTypeField) {
            Record<GenericObject> records = PrimitiveRecordsUtils.getRecord(primitiveTestWrapper.getType(), null);
            List<Field> fields = primitiveSchemaConvert.convertUserSchema(records);
            for (Field field : fields) {
                if (field.getName().equals(DefaultSystemFieldConvert.PRIMITIVE_VALUE_NAME)) {
                    Assert.assertEquals(field.getType(), primitiveTestWrapper.getAssertType());
                }
            }
        }
    }

    @Data
    @AllArgsConstructor
    class PrimitiveTestWrapper {
        private Schema type;
        private LegacySQLTypeName assertType;
    }
}