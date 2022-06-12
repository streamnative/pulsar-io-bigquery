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

import static org.apache.pulsar.ecosystem.io.bigquery.convert.DefaultSystemFieldConvert.PRIMITIVE_VALUE_NAME;
import com.google.cloud.bigquery.Field;
import com.google.cloud.bigquery.StandardSQLTypeName;
import com.google.common.collect.Lists;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.pulsar.client.api.schema.GenericObject;
import org.apache.pulsar.common.schema.SchemaType;
import org.apache.pulsar.ecosystem.io.bigquery.convert.logicaltype.PrimitiveLogicalFieldConvert;
import org.apache.pulsar.functions.api.Record;

/**
 * primitive schema convert.
 */
public class PrimitiveSchemaConvert extends AbstractSchemaConvert {

    private static final Map<SchemaType, StandardSQLTypeName> PRIMITIVE_TYPE_MAP;
    private static final PrimitiveLogicalFieldConvert logicalFieldConvert;

    static {
        logicalFieldConvert = PrimitiveLogicalFieldConvert.getInstance();
        PRIMITIVE_TYPE_MAP = new HashMap<>();
        PRIMITIVE_TYPE_MAP.put(SchemaType.STRING, StandardSQLTypeName.STRING);
        PRIMITIVE_TYPE_MAP.put(SchemaType.BOOLEAN, StandardSQLTypeName.BOOL);
        PRIMITIVE_TYPE_MAP.put(SchemaType.INT8, StandardSQLTypeName.INT64);
        PRIMITIVE_TYPE_MAP.put(SchemaType.INT16, StandardSQLTypeName.INT64);
        PRIMITIVE_TYPE_MAP.put(SchemaType.INT32, StandardSQLTypeName.INT64);
        PRIMITIVE_TYPE_MAP.put(SchemaType.INT64, StandardSQLTypeName.INT64);
        PRIMITIVE_TYPE_MAP.put(SchemaType.FLOAT, StandardSQLTypeName.FLOAT64);
        PRIMITIVE_TYPE_MAP.put(SchemaType.DOUBLE, StandardSQLTypeName.FLOAT64);
        PRIMITIVE_TYPE_MAP.put(SchemaType.BYTES, StandardSQLTypeName.BYTES);
    }

    public PrimitiveSchemaConvert(Set<String> systemFieldNames) {
        super(systemFieldNames);
    }

    public static boolean isPrimitiveSchema(SchemaType schemaType) {
        return PRIMITIVE_TYPE_MAP.containsKey(schemaType) || logicalFieldConvert.isLogicType(schemaType);
    }

    @Override
    protected List<Field> convertUserSchema(Record<GenericObject> record) {
        SchemaType type = record.getSchema().getSchemaInfo().getType();
        StandardSQLTypeName standardSQLTypeName;
        if (logicalFieldConvert.isLogicType(type)) {
            standardSQLTypeName = logicalFieldConvert.convertFieldType(type);
        } else {
            standardSQLTypeName = PRIMITIVE_TYPE_MAP.get(type);
        }
        Field field = Field.newBuilder(PRIMITIVE_VALUE_NAME, standardSQLTypeName)
                .setMode(Field.Mode.NULLABLE).build();
        return Lists.newArrayList(field);
    }
}
