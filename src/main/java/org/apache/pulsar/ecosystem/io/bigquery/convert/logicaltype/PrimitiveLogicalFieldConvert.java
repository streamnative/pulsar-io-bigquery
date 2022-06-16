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
package org.apache.pulsar.ecosystem.io.bigquery.convert.logicaltype;

import com.google.cloud.bigquery.StandardSQLTypeName;
import com.google.cloud.bigquery.storage.v1.CivilTimeEncoder;
import java.sql.Time;
import java.sql.Timestamp;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import org.apache.pulsar.common.schema.SchemaType;
import org.apache.pulsar.ecosystem.io.bigquery.exception.BQConnectorDirectFailException;
import org.apache.pulsar.ecosystem.io.bigquery.utils.TimeUtils;

/**
 * primitive logical field convert.
 */
public class PrimitiveLogicalFieldConvert implements LogicalFieldConvert<SchemaType>{

    private static final Map<SchemaType, StandardSQLTypeName> logicalFields;

    static {
        logicalFields = new HashMap<>();
        logicalFields.put(SchemaType.DATE, StandardSQLTypeName.TIMESTAMP);
        logicalFields.put(SchemaType.TIMESTAMP, StandardSQLTypeName.TIMESTAMP);
        logicalFields.put(SchemaType.TIME, StandardSQLTypeName.TIMESTAMP);
        logicalFields.put(SchemaType.INSTANT, StandardSQLTypeName.DATETIME);
        logicalFields.put(SchemaType.LOCAL_DATE, StandardSQLTypeName.DATE);
        logicalFields.put(SchemaType.LOCAL_TIME, StandardSQLTypeName.TIME);
        logicalFields.put(SchemaType.LOCAL_DATE_TIME, StandardSQLTypeName.DATETIME);
    }

    @Override
    public StandardSQLTypeName convertFieldType(SchemaType pulsarFieldType) {
        StandardSQLTypeName value = logicalFields.get(pulsarFieldType);
        if (value == null) {
            throw new BQConnectorDirectFailException(
                    "Primitive schema not support logical type:" + pulsarFieldType);
        }
        return value;
    }

    @Override
    public Object convertFieldValue(SchemaType pulsarFieldType, Object pulsarFieldValue) {
        StandardSQLTypeName tableFleType = logicalFields.get(pulsarFieldType);
        switch (tableFleType) {
            case TIMESTAMP:
                // bigquery receive micros timestamps
                if (pulsarFieldValue instanceof Date) {
                    return ((Date) pulsarFieldValue).getTime() * 1000;
                }
                if (pulsarFieldValue instanceof Timestamp) {
                    return ((Timestamp) pulsarFieldValue).getTime() * 1000;
                }
                if (pulsarFieldValue instanceof Time) {
                    return ((Time) pulsarFieldValue).getTime() * 1000;
                }
                break;
            case TIME:
                if (pulsarFieldValue instanceof LocalTime) {
                    return CivilTimeEncoder.encodePacked64TimeMicros(TimeUtils.convertLocalTime(
                            (LocalTime) pulsarFieldValue));
                }
                break;
            case DATETIME:
                if (pulsarFieldValue instanceof Instant) {
                    org.threeten.bp.LocalDateTime localDateTime =
                               TimeUtils.convertLocalDateTime(((Instant) pulsarFieldValue).toEpochMilli());
                    return CivilTimeEncoder.encodePacked64DatetimeMicros(localDateTime);
                }
                if (pulsarFieldValue instanceof LocalDateTime) {
                    org.threeten.bp.LocalDateTime localDateTime =
                            TimeUtils.convertLocalDateTime((LocalDateTime) pulsarFieldValue);
                    return CivilTimeEncoder.encodePacked64DatetimeMicros(localDateTime);
                }
                break;
            case DATE:
                if (pulsarFieldValue instanceof LocalDate) {
                    return (int) ((LocalDate) pulsarFieldValue).toEpochDay();
                }
                break;
        }
        throw new BQConnectorDirectFailException(
                String.format("Not support logical type: %s, value class is: %s", pulsarFieldType,
                        pulsarFieldValue.getClass()));

    }

    @Override
    public boolean isLogicType(SchemaType pulsarFieldType) {
        return logicalFields.containsKey(pulsarFieldType);
    }
}
