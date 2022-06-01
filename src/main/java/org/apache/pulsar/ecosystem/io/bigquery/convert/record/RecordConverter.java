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

import com.google.cloud.bigquery.storage.v1.ProtoRows;
import com.google.cloud.bigquery.storage.v1.TableFieldSchema;
import com.google.protobuf.Descriptors;
import java.util.List;
import org.apache.pulsar.client.api.schema.GenericObject;
import org.apache.pulsar.ecosystem.io.bigquery.exception.RecordConvertException;
import org.apache.pulsar.functions.api.Record;

/**
 * record converter.
 */
public interface RecordConverter {

    /**
     * Converter record.
     *
     * @param record pulsar record
     * @param protoSchema protobuf descriptor
     * @param tableFieldSchema table field schemas
     * @return
     * @throws RecordConvertException
     *  1. When there are fields in Pulsar that are not in BigQuery.
     *  2. When there is a field in BigQuery that is not in Pulsar, and it is set to a required type.
     */
    ProtoRows convertRecord(Record<GenericObject> record,
                            Descriptors.Descriptor protoSchema,
                            List<TableFieldSchema> tableFieldSchema) throws RecordConvertException;

}
