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
                    + "it will use __message_id__ the partition key.")
    private boolean partitionedTables;

    @FieldDoc(required = false,
            defaultValue = "7",
            help = "clusteredTableIntervalDay is number of days between partitioning of the partitioned table")
    private int partitionedTableIntervalDay;

    @FieldDoc(required = false,
            defaultValue = "false",
            help = "Create a clusteredTables table when the table is automatically created,"
                    + "it will use __event_time__ the partition key.")
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
    private String keyJson;

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
        if (clusteredTables) {
            fields.add("__event_time__");
        }
        if (partitionedTables) {
            fields.add("__message_id__");
        }
        return fields;
    }

    public BigQuery createBigQuery() throws IOException {
        if (keyJson != null && !keyJson.isEmpty()) {
            return BigQueryOptions.newBuilder().setCredentials(getGoogleCredentials()).build().getService();
        } else {
            return BigQueryOptions.getDefaultInstance().getService();
        }
    }

    public BigQueryWriteClient createBigQueryWriteClient() throws IOException {
        if (keyJson != null && !keyJson.isEmpty()) {
            BigQueryWriteSettings settings =
                    BigQueryWriteSettings.newBuilder().setCredentialsProvider(() -> getGoogleCredentials()).build();
            return BigQueryWriteClient.create(settings);
        } else {
            return BigQueryWriteClient.create();
        }

    }

    private GoogleCredentials getGoogleCredentials() throws IOException {
        GoogleCredentials googleCredentials = GoogleCredentials
                .fromStream(new ByteArrayInputStream(keyJson.getBytes(StandardCharsets.UTF_8)));
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
        bigQueryConfig.setKeyJson("{\n"
                + "  \"type\": \"service_account\",\n"
                + "  \"project_id\": \"affable-ray-226821\",\n"
                + "  \"private_key_id\": \"f21f91c574f20a269ee0ff97491d5a7ce13dcab7\",\n"
                + "  \"private_key\": \"-----BEGIN PRIVATE "
                + "KEY-----\\nMIIEvQIBADANBgkqhkiG9w0BAQEFAASCBKcwggSjAgEAAoIBAQDFbGej6ujSKr8M"
                + "\\nkFq2xKaTNB9V31X3TqBPl/hhMGKOed3TXtaFhVIhlTFBQRF/Qt4aXE9gSsvq+6k"
                + "/\\ntpF7U1ZsFCNgB90WRneIwuh6LiX8EbqovSW2k7px4gLOfkWMk6L/HUSbTLGgDXvk\\n0/idAOSFsRNOfqKHAUnTEKSESJp"
                + "+JSOF7yPbBMwtG/vne4O97cUluPkuyIMWMktq\\nVeva02VEFTJQSpa9cp"
                + "/7CYYr1j1Xr3MusYxKgyCCkj80927Wo7onGkL7UshtWIxV"
                + "\\nTn56frxqtRsqNcKxJ0n4QWBuNIZvzAlAsWxhAX1adBsXxtBulppjjWdQ7tsYWMk7"
                + "\\nWdmMtOvPAgMBAAECggEAGGC48Xw10JHXsnj2wp/Iw9+JSQTBbD7Iu9tTn8imOtqg\\nghOxYJ5kVGJEST78Jp8"
                + "+prmkUYsa4ALAVO45y6UwRDs+XQLLkY8U/o22wDOHnDci\\nOejVSdS3Do3uc8oS03d9hov6J0USn+VaWZi1F8n+7eU"
                + "+MZCgiBQoJghlkDIY5Z9/\\n2y5oNljIJt+uHPLRXUjS8Gl5RQKriuvporXhP/paglZO5v65EDgHrhFpnVOqDykf\\nUQOGij"
                + "+P/cSKAdfinj2sLFvKeXlN/GPFe6oeR/E4wvt+BcgMJxRx80jVWF6Cb2N1"
                + "\\nA4d5OvxenG7uQe7WWLUbHqxYWSrPk8bxNKtaSE2BVQKBgQDkJy6Pb1/6GVngf6sk"
                + "\\nKDFUwQt3n0cFvq2OIrNBDDPort28x5nZn7Wp+AIZ4myBGciaxIF7yAKmXzWViIC8"
                + "\\nRJU4uTYIsl5x49BNbNWClEvPfjSZYJt8foNBUX9zM8FPSG03tE3oI+HfwC/A+9VD\\n0seu4JaCf+zU0"
                + "+78gyvwP16tkwKBgQDdhQ2QpeH0LIh8WDRkEuAJSjPh70vSCiSB\\nmRPG0t/g56EsUp"
                + "+fbeV0vEaviJf032OEP2RCX1pduFu0fw5ee5ZQZS1rMu2d9g+k\\n7jYEGzvO6Y1NiPCnT5U+bB1QWQd1qo"
                + "+aA9L8xDbdHVOHxwjGl76qWiAxDfZHy3WN"
                + "\\nZmcruqPOVQKBgH7mXDtjk1qkZx07ZZGC2Y1uolYyvWowmJAKNPHlO6ocOTEbRDOL\\nZZvYWjLTgc587NtImUyj"
                + "/vVS15cIibIt42HdgnRr4aQvNlkaQ9eRbGlIpTD6TwF7\\nzL2z"
                + "+tO04bybaEQngX2xF51AZE9Ow1wqDO7z9EM5rBzklHTW8MV8OckJAoGBAI+p"
                + "\\nxcVkv6jC5PV3ouqwDxoXRJSnxA2BRHHEXD4a7tUGg9Gh+CHGfQR7QoB+3sYRxpGz\\nJwugmZOkh90EdcYy8qZLJ+yZB9/k"
                + "//pf/yokm2Iyt9BsGl73zvu/1DcStjLKnf0t\\ns7z4Z/h5F348R+W77gy/DJejzXB8dE1y90A1+BxNAoGAdMKDD3jJ"
                + "+Rw5STgLgfSs\\nJ+3Bp75Ftj82G+3Uk7Ol0zRLd1sagQTOCP0SsBt2WXUZW9s8IXBdMVpAUb6/ssi8"
                + "\\nspDL3y2eFmznp6T1Ygz9WQSkWeiuwZHSVuNO+FydFaeg6ynz4qsw6Oc72Y+sGzqz\\nMiZqDNmoLwHISTyUiHpcyeI=\\n"
                + "-----END PRIVATE KEY-----\\n\",\n"
                + "  \"client_email\": \"baodi-shi-bigquery-connector@affable-ray-226821.iam.gserviceaccount.com\",\n"
                + "  \"client_id\": \"114269280785330516951\",\n"
                + "  \"auth_uri\": \"https://accounts.google.com/o/oauth2/auth\",\n"
                + "  \"token_uri\": \"https://oauth2.googleapis.com/token\",\n"
                + "  \"auth_provider_x509_cert_url\": \"https://www.googleapis.com/oauth2/v1/certs\",\n"
                + "  \"client_x509_cert_url\": \"https://www.googleapis"
                + ".com/robot/v1/metadata/x509/baodi-shi-bigquery-connector%40affable-ray-226821.iam.gserviceaccount"
                + ".com\"\n"
                + "}");
        return bigQueryConfig;
    }
}
