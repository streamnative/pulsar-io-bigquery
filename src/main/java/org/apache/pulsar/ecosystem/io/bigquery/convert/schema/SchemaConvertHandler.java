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

import com.google.cloud.bigquery.Schema;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import org.apache.pulsar.client.api.schema.GenericObject;
import org.apache.pulsar.common.schema.SchemaType;
import org.apache.pulsar.ecosystem.io.bigquery.exception.BQConnectorSchemaException;
import org.apache.pulsar.functions.api.Record;

/**
 * Schema convert handler.
 */
public class SchemaConvertHandler implements SchemaConvert {

    private final Map<SchemaType, SchemaConvert> schemaConverts = new HashMap<>();
    private final PrimitiveSchemaConvert primitiveSchemaConvert;

    public SchemaConvertHandler(Set<String> systemFieldNames) {
        schemaConverts.put(SchemaType.AVRO, new AvroSchemaConvert(systemFieldNames));
        primitiveSchemaConvert = new PrimitiveSchemaConvert(systemFieldNames);
    }

    @Override
    public Schema convertSchema(Record<GenericObject> records) {
        SchemaType type = records.getSchema().getSchemaInfo().getType();

        if (type.isPrimitive()) {
            return primitiveSchemaConvert.convertSchema(records);
        }

        SchemaConvert schemaConvert = schemaConverts.get(type);
        if (schemaConvert == null) {
            throw new BQConnectorSchemaException("Not support schema type: " + type);
        }
        return schemaConvert.convertSchema(records);
    }
}
