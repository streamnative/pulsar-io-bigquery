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
import com.google.cloud.bigquery.Schema;
import com.google.cloud.bigquery.StandardSQLTypeName;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.client.api.schema.GenericObject;
import org.apache.pulsar.ecosystem.io.bigquery.convert.DefaultSystemFieldConvert;
import org.apache.pulsar.functions.api.Record;

/**
 * Abstract schema convert.
 */
@Slf4j
public abstract class AbstractSchemaConvert implements SchemaConvert {

    private final Set<String> systemFieldNames;

    public AbstractSchemaConvert(Set<String> systemFieldNames) {
        this.systemFieldNames = systemFieldNames;
    }

    @Override
    public Schema convertSchema(Record<GenericObject> record) {
        List<Field> systemField = convertSystemField();
        systemField.addAll(convertUserSchema(record));
        return Schema.of(systemField);
    }

    /**
     * Convert user record.
     *
     * @param record
     * @return
     */
    protected abstract List<Field> convertUserSchema(Record<GenericObject> record);

    private List<Field> convertSystemField() {
        return Optional.ofNullable(systemFieldNames)
                .map(systemFieldNames -> systemFieldNames.stream()
                        .map(systemFieldName -> {
                            StandardSQLTypeName fieldSQLType =
                                    DefaultSystemFieldConvert.getFieldSQLType(systemFieldName);
                            if (fieldSQLType == null) {
                                log.warn("Not support system field [{}], ignore this field.", systemFieldName);
                                return null;
                            } else {
                                return Field.newBuilder(systemFieldName, fieldSQLType)
                                                       .setMode(Field.Mode.NULLABLE).build();
                            }
                        }).filter(field -> field != null)
                        .collect(Collectors.toList())
                ).orElse(new ArrayList<>());
    }
}
