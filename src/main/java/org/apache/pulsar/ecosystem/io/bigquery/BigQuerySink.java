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

import com.google.cloud.bigquery.storage.v1.BigQueryWriteClient;
import com.google.cloud.bigquery.storage.v1.CreateWriteStreamRequest;
import com.google.cloud.bigquery.storage.v1.FinalizeWriteStreamRequest;
import com.google.cloud.bigquery.storage.v1.ProtoRows;
import com.google.cloud.bigquery.storage.v1.StreamWriter;
import com.google.cloud.bigquery.storage.v1.TableName;
import com.google.cloud.bigquery.storage.v1.WriteStream;
import java.util.Map;
import java.util.Objects;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.client.api.schema.GenericRecord;
import org.apache.pulsar.ecosystem.io.bigquery.convert.record.RecordConverter;
import org.apache.pulsar.ecosystem.io.bigquery.convert.record.RecordConverterHandler;
import org.apache.pulsar.ecosystem.io.bigquery.exception.BigQueryConnectorRuntimeException;
import org.apache.pulsar.ecosystem.io.bigquery.exception.RecordConvertException;
import org.apache.pulsar.functions.api.Record;
import org.apache.pulsar.io.core.Sink;
import org.apache.pulsar.io.core.SinkContext;

/**
 * BigQuery Sink impl.
 */
@Slf4j
public class BigQuerySink implements Sink<GenericRecord> {

    // bigquery
    private BigQueryWriteClient client;
    private WriteStream writeStream;
    private StreamWriter streamWriter;
    private TableName tableName;

    // pulsar
    private RecordConverter recordConverter;
    private BigQueryConfig bigQueryConfig;
    private SchemaManager schemaManager;

    // is init bq resources
    private boolean init;

    @Override
    public void open(Map<String, Object> config, SinkContext sinkContext) throws Exception {
        this.bigQueryConfig = BigQueryConfig.load(config);
        Objects.requireNonNull(bigQueryConfig.getProjectId(), "BigQuery project id is not set");
        Objects.requireNonNull(bigQueryConfig.getDatasetName(), "BigQuery dataset id is not set");
        Objects.requireNonNull(bigQueryConfig.getTableName(), "BigQuery table name id is not set");

        this.client = bigQueryConfig.createBigQueryWriteClient();
        this.tableName = TableName.of(bigQueryConfig.getProjectId(),
                bigQueryConfig.getDatasetName(), bigQueryConfig.getTableName());
        this.recordConverter = new RecordConverterHandler();
        this.schemaManager = new SchemaManager(bigQueryConfig);
    }

    @Override
    public void write(Record<GenericRecord> record) throws Exception {

        // 0. Try to create table and init bigquery resources
        if (!init) {
            schemaManager.initTable(record);
            tryUpdateBigqueryResources();
            init = true;
        }

        // 2. Write record.
        writeRecord(record);

        // 3. Ack message.
        log.debug("Append success, ack this message <{}>", record.getMessage().get().getMessageId());
        record.ack();
    }

    @Override
    public void close() {
        if (streamWriter != null) {
            streamWriter.close();
        }
        if (writeStream != null) {
            // Finalize the stream after use.
            FinalizeWriteStreamRequest finalizeWriteStreamRequest =
                    FinalizeWriteStreamRequest.newBuilder().setName(writeStream.getName()).build();
            client.finalizeWriteStream(finalizeWriteStreamRequest);
        }
    }

    private void writeRecord(Record<GenericRecord> record) {

        // convert record and try update schema.
        ProtoRows protoRows = convertRecord(record);

        // Try first append rows.
        try {
            streamWriter.append(protoRows).get();
            return;
        } catch (Exception e) {
            // TODO Refinement exceptions, other exceptions, throw exceptions directly
            log.warn("Append record field, try update schema: <{}>", e.getMessage());
            schemaManager.updateSchema(record);
        }

        // Bigquery resource update is delayed, try a few more times.
        try {
            tryUpdateBigqueryResources();
            streamWriter.append(protoRows).get();
            return;
        } catch (Exception e) {
            // TODO Refinement exceptions, other exceptions, throw exceptions directly
            throw new BigQueryConnectorRuntimeException(
                    "Append record failed, after trying to update the schema it still fails");
        }

    }

    private ProtoRows convertRecord(Record<GenericRecord> record) {
        try {
            return recordConverter.convertRecord(record, schemaManager.getDescriptor(),
                    schemaManager.getTableSchema().getFieldsList());
        } catch (RecordConvertException e) {
            // Not care why exception, try to update the schema directly and get the latest tableSchema.
            log.warn("Convert failed to record, try update schema: <{}>", e.getMessage());
            schemaManager.updateSchema(record);
        }

        // Bigquery resource update is delayed, try a few more times.
        try {
            tryUpdateBigqueryResources();
            return recordConverter.convertRecord(record, schemaManager.getDescriptor(),
                    schemaManager.getTableSchema().getFieldsList());
        } catch (Exception e) {
            throw new BigQueryConnectorRuntimeException(
                    "Convert record failed, after trying to update the schema it still fails", e);
        }
    }

    private void tryUpdateBigqueryResources() throws Exception {
        int tryCount = 0;
        while (true) {
            Thread.sleep(5000);
            try {
                updateBigQueryResources();
                return;
            } catch (Exception e) {
                if (tryCount == 5) {
                    throw new BigQueryConnectorRuntimeException(
                            "Update big query resources failed, it doesn't work even after 5 tries, please check", e);
                } else {
                    tryCount++;
                    log.warn("Update big query resources count <{}> failed, Retry after 5 seconds <{}>",
                            tryCount, e.getMessage());
                }
            }
        }
    }

    private void updateBigQueryResources() throws Exception {
        close();
        CreateWriteStreamRequest createWriteStreamRequest =
                CreateWriteStreamRequest.newBuilder()
                        .setParent(this.tableName.toString())
                        .setWriteStream(WriteStream.newBuilder().setType(WriteStream.Type.COMMITTED).build())
                        .build();
        // if table not found, client will throw exception
        writeStream = client.createWriteStream(createWriteStreamRequest);
        streamWriter = StreamWriter
                .newBuilder(writeStream.getName(), client)
                .setWriterSchema(schemaManager.getProtoSchema())
                .build();
        log.info("Update resources success, start new write stream: {}", writeStream.getName());
    }
}
