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

import com.google.common.collect.Lists;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.pulsar.shade.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.pulsar.shade.com.fasterxml.jackson.dataformat.yaml.YAMLFactory;

/**
 * Big query config.
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class BigQueryConfig {

    private String projectId;

    private String datasetName;

    private String tableName;

    private boolean partitionedTables;

    private boolean clusteredTables;

    private boolean autoCreateTable;

    private boolean autoUpdateTable;

    private String defaultSystemField;


    public List<String> getDefaultSystemField() {
        return Optional.ofNullable(defaultSystemField)
                .map(field -> Lists.newArrayList(defaultSystemField.split(",")))
                .orElse(new ArrayList<>());
    }


    public static BigQueryConfig load(String yamlFile) throws IOException {
        ObjectMapper mapper = new ObjectMapper(new YAMLFactory());
        return mapper.readValue(new File(yamlFile), BigQueryConfig.class);
    }

    public static BigQueryConfig load(Map<String, Object> map) throws IOException {
        ObjectMapper mapper = new ObjectMapper();
        return mapper.readValue(new ObjectMapper().writeValueAsString(map), BigQueryConfig.class);
    }
}
