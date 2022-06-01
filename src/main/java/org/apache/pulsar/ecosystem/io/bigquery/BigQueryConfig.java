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

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.BigQueryOptions;
import com.google.cloud.bigquery.TableId;
import com.google.cloud.bigquery.storage.v1.BigQueryWriteClient;
import com.google.cloud.bigquery.storage.v1.BigQueryWriteSettings;
import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import lombok.Data;
import org.apache.pulsar.ecosystem.io.bigquery.exception.BigQueryConnectorRuntimeException;
import org.apache.pulsar.io.core.annotations.FieldDoc;

/**
 * Big query config.
 */
@Data
public class BigQueryConfig implements Serializable {

    private static final long serialVersionUID = 1L;

    @FieldDoc(required = true,
            defaultValue = "",
            help = "projectId is BigQuery project id")
    private String projectId;

    @FieldDoc(required = true,
            defaultValue = "",
            help = "datasetName is BigQuery dataset name")
    private String datasetName;

    @FieldDoc(required = true,
            defaultValue = "",
            help = "tableName is BigQuery table name")
    private String tableName;

    @FieldDoc(required = false,
            defaultValue = "false",
            help = "Create a partitioned table when the table is automatically created,"
                    + "it will use __event_time__ the partition key.")
    private boolean partitionedTables;

    @FieldDoc(required = false,
            defaultValue = "7",
            help = "partitionedTableIntervalDay is number of days between partitioning of the partitioned table")
    private int partitionedTableIntervalDay;

    @FieldDoc(required = false,
            defaultValue = "false",
            help = "Create a clusteredTables table when the table is automatically created,"
                    + "it will use __message_id__ the partition key.")
    private boolean clusteredTables;

    @FieldDoc(required = false,
            defaultValue = "false",
            help = "Automatically create table when table does not exist")
    private boolean autoCreateTable;

    @FieldDoc(required = false,
            defaultValue = "false",
            help = "Automatically update table schema when table schema is incompatible")
    private boolean autoUpdateTable;

    @FieldDoc(required = false,
            defaultValue = "",
            help = "Create system fields when the table is automatically created, separate multiple fields with commas."
                    + " The supported system fields are: __schema_version__ , __partition__ , __event_time__ ,"
                    + " __publish_time__ , __message_id__ , __sequence_id__ , __producer_name__")
    private String defaultSystemField;

    @FieldDoc(required = false,
            defaultValue = "",
            help = "Authentication key, use the environment variable to get the key when key is empty."
                    + " Key acquisition reference: \n"
                    + "https://cloud.google.com/bigquery/docs/quickstarts/quickstart-client-libraries#before-you-begin")
    private String credentialJsonString;

    public TableId getTableId() {
        return TableId.of(projectId, datasetName, tableName);
    }

    public Set<String> getDefaultSystemField() {
        Set<String> fields = Optional.ofNullable(defaultSystemField)
                .map(__ -> Arrays.stream(defaultSystemField.split(","))
                        .map(field -> {
                            String trim = field.trim();
                            if (trim.contains(" ")) {
                                throw new BigQueryConnectorRuntimeException(
                                        "There cannot be spaces in the field: " + defaultSystemField);
                            }
                            return trim;
                        }).collect(Collectors.toSet())
                ).orElse(new HashSet<>());
        if (partitionedTables) {
            fields.add("__event_time__");
        }
        if (clusteredTables) {
            fields.add("__message_id__");
        }
        return fields;
    }

    public BigQuery createBigQuery() throws IOException {
        if (credentialJsonString != null && !credentialJsonString.isEmpty()) {
            return BigQueryOptions.newBuilder().setCredentials(getGoogleCredentials()).build().getService();
        } else {
            return BigQueryOptions.getDefaultInstance().getService();
        }
    }

    public BigQueryWriteClient createBigQueryWriteClient() throws IOException {
        if (credentialJsonString != null && !credentialJsonString.isEmpty()) {
            BigQueryWriteSettings settings =
                    BigQueryWriteSettings.newBuilder().setCredentialsProvider(() -> getGoogleCredentials()).build();
            return BigQueryWriteClient.create(settings);
        } else {
            return BigQueryWriteClient.create();
        }

    }

    private GoogleCredentials getGoogleCredentials() throws IOException {
        GoogleCredentials googleCredentials = GoogleCredentials
                .fromStream(new ByteArrayInputStream(credentialJsonString.getBytes(StandardCharsets.UTF_8)));
        return googleCredentials;
    }

    public static BigQueryConfig load(String yamlFile) throws IOException {
        ObjectMapper mapper = new ObjectMapper(new YAMLFactory());
        return mapper.readValue(new File(yamlFile), BigQueryConfig.class);
    }

    public static BigQueryConfig load(Map<String, Object> map) throws IOException {
        ObjectMapper mapper = new ObjectMapper();
        BigQueryConfig bigQueryConfig =
                mapper.readValue(new ObjectMapper().writeValueAsString(map), BigQueryConfig.class);
        return bigQueryConfig;
    }
}
