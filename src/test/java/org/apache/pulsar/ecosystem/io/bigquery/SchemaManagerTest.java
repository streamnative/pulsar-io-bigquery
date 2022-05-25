package org.apache.pulsar.ecosystem.io.bigquery;

import static java.net.HttpURLConnection.HTTP_NOT_FOUND;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;
import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.BigQueryException;
import com.google.cloud.bigquery.Field;
import com.google.cloud.bigquery.Schema;
import com.google.cloud.bigquery.StandardTableDefinition;
import com.google.cloud.bigquery.Table;
import com.google.cloud.bigquery.TableInfo;
import com.google.cloud.bigquery.TimePartitioning;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import lombok.Getter;
import lombok.Setter;
import lombok.SneakyThrows;
import org.apache.pulsar.client.api.schema.GenericRecord;
import org.apache.pulsar.ecosystem.io.bigquery.exception.BigQueryConnectorRuntimeException;
import org.apache.pulsar.functions.api.Record;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

/**
 * schema manager test.
 */
public class SchemaManagerTest {

    BigQueryConfig bigQueryConfig;

    @Before
    public void step() {
        bigQueryConfig = spy(BigQueryConfig.class);
        bigQueryConfig.setProjectId("project_id");
        bigQueryConfig.setDatasetName("dateset_name");
        bigQueryConfig.setTableName("table_name");
    }

    @SneakyThrows
    @Test
    public void testInit() {
        BigQueryMock bigQueryMock = new BigQueryMock();
        when(bigQueryConfig.createBigQuery()).thenReturn(bigQueryMock.getBigQuery());
        bigQueryConfig.setAutoCreateTable(false);

        // Mock table not exit.
        when(bigQueryMock.getBigQuery().getTable(Mockito.any()))
                .thenThrow(new BigQueryException(HTTP_NOT_FOUND, "Not found table"));
        try {
            new SchemaManager(bigQueryConfig);
            fail("Should has failed");
        } catch (Exception e) {
        }

        bigQueryConfig.setAutoCreateTable(true);
        new SchemaManager(bigQueryConfig);
    }

    @SneakyThrows
    @Test
    public void testCreateAndUpdateTable() {
        BigQueryMock bigQueryMock = new BigQueryMock();
        when(bigQueryConfig.createBigQuery()).thenReturn(bigQueryMock.getBigQuery());
        bigQueryConfig.setAutoCreateTable(true);
        bigQueryConfig.setAutoUpdateTable(true);
        bigQueryConfig.setClusteredTables(true);
        bigQueryConfig.setPartitionedTables(true);
        bigQueryConfig.setPartitionedTableIntervalDay(10);

        SchemaManager schemaManager = new SchemaManager(bigQueryConfig);
        Record<GenericRecord> genericRecordRecordFirst = AvroRecordsUtils.getGenericRecordRecordFirst();

        // create table
        schemaManager.createTable(genericRecordRecordFirst);
        StandardTableDefinition tableDefinition = bigQueryMock.getTableDefinition();

        // auto create cluster table
        List<String> clusterField = tableDefinition.getClustering().getFields();
        assertEquals(clusterField.size(), 1);
        assertEquals(clusterField.get(0), "__message_id__");

        // auto create partition table
        TimePartitioning timePartitioning = tableDefinition.getTimePartitioning();
        assertEquals(timePartitioning.getField(), "__event_time__");
        assertTrue(timePartitioning.getExpirationMs()
                            == TimeUnit.MILLISECONDS.convert(10, TimeUnit.DAYS));

        Schema schema = bigQueryMock.getSchema();
        assertNotNull(schema);
        assertNotNull(schema.getFields().get("col3"));
        try {
            schema.getFields().get("col4");
            fail("Should has failed");
        } catch (IllegalArgumentException e) {
            // not found col4
        }

        // set col3 model as REQUIRED
        Set<Field> fields = new HashSet<>(schema.getFields());
        Field col3 = schema.getFields().get("col3");
        fields.remove(col3);
        Field newCol3 = col3.toBuilder().setMode(Field.Mode.REQUIRED).build();
        fields.add(newCol3);
        Schema newSchema = Schema.of(fields);
        bigQueryMock.setTableDefinition(
                bigQueryMock.getTableDefinition().toBuilder().setSchema(newSchema).build());
        bigQueryMock.setSchema(Schema.of(fields));

        // disable auto schema, should throw exception.
        bigQueryConfig.setAutoUpdateTable(false);
        SchemaManager schemaManager2 = new SchemaManager(bigQueryConfig);
        try {
            schemaManager2.updateSchema(genericRecordRecordFirst);
            fail("Should has failed");
        } catch (BigQueryConnectorRuntimeException e) {
        }

        // update schema, add col4 to schema and update col3 model to NULLABLE
        Record<GenericRecord> genericRecordRecordSecond = AvroRecordsUtils.getGenericRecordRecordSecond();
        schemaManager.updateSchema(genericRecordRecordSecond);
        Schema allSchema = bigQueryMock.getSchema();
        assertNotNull(allSchema.getFields().get("col3"));
        assertEquals(allSchema.getFields().get("col3").getMode(), Field.Mode.NULLABLE);
        assertNotNull(allSchema.getFields().get("col4"));
    }


    @Setter
    @Getter
    static class BigQueryMock {

        private BigQuery bigQuery;

        private StandardTableDefinition tableDefinition;
        private Schema schema;

        public BigQueryMock() {
            this.bigQuery = mock(BigQuery.class);
            when(bigQuery.create(Mockito.any(TableInfo.class), Mockito.any()))
                    .then(invocationOnMock -> {
                        TableInfo tableInfo = invocationOnMock.getArgument(0);
                        tableDefinition = tableInfo.getDefinition();
                        schema = tableDefinition.getSchema();
                        return null;
                    });

            when(bigQuery.getTable(Mockito.any()))
                    .then(invocationOnMock -> {
                        if (tableDefinition == null) {
                            throw new BigQueryException(HTTP_NOT_FOUND, "Not found table");
                        } else {
                            Table table = mock(Table.class);
                            when(table.getDefinition()).thenReturn(tableDefinition);
                            return table;
                        }
                    });

            when(bigQuery.update(Mockito.any(TableInfo.class), Mockito.any()))
                    .then(invocationOnMock -> {
                        TableInfo tableInfo = invocationOnMock.getArgument(0);
                        tableDefinition = tableInfo.getDefinition();
                        schema = tableDefinition.getSchema();
                        return null;
                    });
        }
    }

}