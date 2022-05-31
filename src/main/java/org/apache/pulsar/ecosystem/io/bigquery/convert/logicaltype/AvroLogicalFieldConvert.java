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
import com.google.cloud.bigquery.storage.v1.BigDecimalByteStringEncoder;
import com.google.cloud.bigquery.storage.v1.CivilTimeEncoder;
import com.google.cloud.bigquery.storage.v1.TableFieldSchema;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import org.apache.avro.LogicalType;
import org.apache.avro.LogicalTypes;
import org.apache.pulsar.ecosystem.io.bigquery.exception.BigQueryConnectorRuntimeException;
import org.apache.pulsar.ecosystem.io.bigquery.utils.TimeUtils;

/**
 * Avro logical field convert.
 */
public class AvroLogicalFieldConvert implements LogicalFieldConvert<LogicalType> {

    private final Map<Class<? extends LogicalType>, TableFieldSchema.Type> logicalFields;

    public AvroLogicalFieldConvert() {
        logicalFields = new HashMap<>();
        logicalFields.put(LogicalTypes.Date.class, TableFieldSchema.Type.DATE);
        // TODO support NUMERIC and BIGNUMRIC
        logicalFields.put(LogicalTypes.Decimal.class, TableFieldSchema.Type.BIGNUMERIC);
        logicalFields.put(LogicalTypes.LocalTimestampMillis.class, TableFieldSchema.Type.DATETIME);
        logicalFields.put(LogicalTypes.LocalTimestampMicros.class, TableFieldSchema.Type.DATETIME);
        logicalFields.put(LogicalTypes.TimeMicros.class, TableFieldSchema.Type.TIME);
        logicalFields.put(LogicalTypes.TimestampMillis.class, TableFieldSchema.Type.TIMESTAMP);
        logicalFields.put(LogicalTypes.TimestampMicros.class, TableFieldSchema.Type.TIMESTAMP);
    }

    @Override
    public StandardSQLTypeName convertFieldType(LogicalType pulsarFieldType) {
        return Optional.ofNullable(logicalFields.get(pulsarFieldType.getClass()))
                .map(v -> StandardSQLTypeName.valueOf(v.name()))
                .orElseThrow(() -> new BigQueryConnectorRuntimeException(
                        "AVRO schema not support logical type:" + pulsarFieldType.getName()));
    }

    @Override
    public Object convertFieldValue(LogicalType pulsarFieldType, Object pulsarFieldValue) {
        TableFieldSchema.Type tableFieldType = logicalFields.get(pulsarFieldType.getClass());
        // convert rule: https://cloud.google.com/bigquery/docs/write-api#data_type_conversions
        switch (tableFieldType) {
            case DATE:
                if (pulsarFieldValue instanceof Integer) {
                    return pulsarFieldValue;
                }
                break;
            case NUMERIC:
            case BIGNUMERIC:
                if (pulsarFieldValue instanceof ByteBuffer) {
                    LogicalTypes.Decimal decimal = (LogicalTypes.Decimal) pulsarFieldType;
                    ByteBuffer byteBuffer = (ByteBuffer) pulsarFieldValue;
                    BigInteger bigInteger = new BigInteger(byteBuffer.array());
                    BigDecimal bigDecimal = new BigDecimal(bigInteger, decimal.getScale());
                    return BigDecimalByteStringEncoder.encodeToBigNumericByteString(bigDecimal);
                }
                break;
            case DATETIME:
                if (pulsarFieldValue instanceof Long) {
                    if (pulsarFieldType instanceof LogicalTypes.LocalTimestampMillis) {
                        return CivilTimeEncoder.encodePacked64DatetimeMicros(TimeUtils.convertLocalDateTime(
                                (Long) pulsarFieldValue));
                    } else if (pulsarFieldType instanceof LogicalTypes.LocalTimestampMicros) {
                        return CivilTimeEncoder.encodePacked64DatetimeMicros(TimeUtils.convertLocalDateTime(
                                (Long) pulsarFieldValue / 1000));
                    }
                }

                break;
            case TIME:
                if (pulsarFieldValue instanceof Long) {
                    if (pulsarFieldType instanceof LogicalTypes.TimeMicros) {
                        return pulsarFieldValue;
                    }
                }
                break;
            case TIMESTAMP:
                if (pulsarFieldValue instanceof Long) {
                    if (pulsarFieldType instanceof LogicalTypes.TimestampMillis) {
                        return (Long) pulsarFieldValue * 1000;
                    } else if (pulsarFieldType instanceof LogicalTypes.TimestampMicros) {
                        return pulsarFieldValue;
                    }
                }
                break;
        }
        throw new BigQueryConnectorRuntimeException("Not support logic type: " + pulsarFieldType
                + ", avro data class: " + pulsarFieldValue.getClass().getName());
    }

    @Override
    public boolean isLogicType(LogicalType pulsarFieldType) {
        return logicalFields.containsKey(pulsarFieldType.getClass());
    }
}
