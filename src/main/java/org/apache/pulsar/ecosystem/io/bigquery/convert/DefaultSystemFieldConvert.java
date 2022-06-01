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
package org.apache.pulsar.ecosystem.io.bigquery.convert;

import com.google.cloud.bigquery.StandardSQLTypeName;
import java.util.HashMap;
import java.util.Map;
import org.apache.pulsar.client.api.schema.GenericObject;
import org.apache.pulsar.ecosystem.io.bigquery.exception.BigQueryConnectorRuntimeException;
import org.apache.pulsar.functions.api.Record;

/**
 * default system field convert.
 */
public final class DefaultSystemFieldConvert {

    public static final String MAP_KEY_NAME = "key";
    public static final String MAP_VALUE_NAME = "value";
    private static final Map<String, StandardSQLTypeName> defaultFieldTypeMapping = new HashMap<>();

    static {
        defaultFieldTypeMapping.put("__schema_version__", StandardSQLTypeName.BYTES);
        defaultFieldTypeMapping.put("__partition__", StandardSQLTypeName.INT64);
        defaultFieldTypeMapping.put("__event_time__", StandardSQLTypeName.TIMESTAMP);
        defaultFieldTypeMapping.put("__publish_time__", StandardSQLTypeName.INT64);
        defaultFieldTypeMapping.put("__message_id__", StandardSQLTypeName.STRING);
        defaultFieldTypeMapping.put("__sequence_id__", StandardSQLTypeName.INT64);
        defaultFieldTypeMapping.put("__producer_name__", StandardSQLTypeName.STRING);
        // TODO support __key__, type is StandardSQLTypeName.STRUCT
    }

    /**
     * @param filedName
     * @return
     */
    public static StandardSQLTypeName getFieldSQLType(String filedName) {
        return defaultFieldTypeMapping.get(filedName);
    }

    /***
     *
     * @param fieldName
     * @return
     */
    public static boolean isSystemField(String fieldName) {
        return defaultFieldTypeMapping.containsKey(fieldName);
    }

    /**
     * @param record
     * @return
     */
    public static Object convert(String fieldName, Record<GenericObject> record) {
        switch (fieldName) {
            case "__schema_version__":
                return record.getMessage().get().getSchemaVersion();
            case "__partition__":
                return record.getPartitionIndex().get();
            case "__event_time__":
                return record.getEventTime().get() * 1000;
            case "__publish_time__":
                return record.getMessage().get().getPublishTime();
            case "__message_id__":
                return record.getMessage().get().getMessageId().toString();
            case "__sequence_id__":
                return record.getMessage().get().getSequenceId();
            case "__producer_name__":
                return record.getMessage().get().getProducerName();
            default:
                throw new BigQueryConnectorRuntimeException("Not support convert system field: " + fieldName);
        }
    }
}
