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
package org.apache.pulsar.ecosystem.io.bigquery;

import com.google.cloud.bigquery.storage.v1.ProtoSchema;
import com.google.protobuf.DynamicMessage;
import java.util.List;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.pulsar.client.api.schema.GenericObject;
import org.apache.pulsar.functions.api.Record;

/**
 * Data Writer.
 */
public interface DataWriter extends AutoCloseable {

    /**
     * append message.
     *
     * @param dataWriterRequests
     * @return
     */
    void append(List<DataWriter.DataWriterRequest> dataWriterRequests);

    /**
     * update resources.
     */
    void updateStream(ProtoSchema protoSchema);

    /**
     * data writer request.
     */
    @Data
    @AllArgsConstructor
    @NoArgsConstructor
    class DataWriterRequest {
        private DynamicMessage dynamicMessage;
        private Record<GenericObject> record;
    }

}
