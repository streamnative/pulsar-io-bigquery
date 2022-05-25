package org.apache.pulsar.ecosystem.io.bigquery.convert.schema;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import com.google.cloud.bigquery.Field;
import com.google.cloud.bigquery.FieldList;
import com.google.cloud.bigquery.LegacySQLTypeName;
import com.google.cloud.bigquery.Schema;
import com.google.common.collect.Sets;
import org.apache.pulsar.client.api.schema.GenericRecord;
import org.apache.pulsar.ecosystem.io.bigquery.AvroRecordsUtils;
import org.apache.pulsar.functions.api.Record;
import org.junit.Test;

/**
 * avro schema convert test.
 */
public class AvroSchemaConvertTest {

    @Test
    public void convertUserSchema() {

        Record<GenericRecord> records = AvroRecordsUtils.getGenericRecordRecordFirst();

        AvroSchemaConvert avroSchemaConvert =
                new AvroSchemaConvert(Sets.newHashSet("__event_time__", "__message_id__", "abc"));
        Schema schema = avroSchemaConvert.convertSchema(records);
        FieldList fields = schema.getFields();

        for (Field field : fields) {
            String name = field.getName();
            switch (name) {
                case "abc":
                    fail("abs is not system field, should ignore.");
                    break;
                case "__event_time__":
                    assertEquals(field.getMode(), Field.Mode.NULLABLE);
                    assertEquals(field.getType(), LegacySQLTypeName.TIMESTAMP);
                    break;
                case "__message_id__":
                case "col1":
                case "col3":
                    assertEquals(field.getMode(), Field.Mode.NULLABLE);
                    assertEquals(field.getType(), LegacySQLTypeName.STRING);
                    break;
                case "tag":
                    assertEquals(field.getMode(), Field.Mode.NULLABLE);
                    assertEquals(field.getType(), LegacySQLTypeName.RECORD);
                    assertTag(field.getSubFields());
                    break;
                case "map3":
                    assertEquals(field.getMode(), Field.Mode.REPEATED);
                    assertEquals(field.getType(), LegacySQLTypeName.RECORD);
                    assertMap3(field.getSubFields());
                    break;
                default:
                    fail("Not found match field name, please check Foo record");
            }

        }

    }

    public void assertTag(FieldList fields) {
        for (Field field : fields) {
            String name = field.getName();
            assertEquals(field.getMode(), Field.Mode.NULLABLE);
            switch (name) {
                case "booleanv":
                    assertEquals(field.getType(), LegacySQLTypeName.BOOLEAN);
                    break;
                case "doublev":
                case "floatv":
                    assertEquals(field.getType(), LegacySQLTypeName.FLOAT);
                    break;
                case "inta":
                case "intb":
                case "bytev":
                case "charv":
                    assertEquals(field.getType(), LegacySQLTypeName.INTEGER);
                    break;
                case "bytesv":
                    assertEquals(field.getType(), LegacySQLTypeName.BYTES);
                    break;
                default:
                    fail("Not found match field name, please check Tag record");
            }
        }

    }

    public void assertDag(FieldList fields) {
        for (Field field : fields) {
            String name = field.getName();
            switch (name) {
                case "test":
                    assertEquals(field.getMode(), Field.Mode.NULLABLE);
                    assertEquals(field.getType(), LegacySQLTypeName.STRING);
                    break;
                case "array":
                    assertEquals(field.getMode(), Field.Mode.REPEATED);
                    assertEquals(field.getType(), LegacySQLTypeName.STRING);
                    break;
                case "bigDecimal":
                    assertEquals(field.getMode(), Field.Mode.NULLABLE);
                    assertEquals(field.getType(), LegacySQLTypeName.BIGNUMERIC);
                    assertTrue(field.getPrecision() == 5);
                    assertTrue(field.getScale() == 2);
                    break;
                case "date":
                    assertEquals(field.getMode(), Field.Mode.NULLABLE);
                    assertEquals(field.getType(), LegacySQLTypeName.DATE);
                    break;
                case "localTimesMillis":
                    assertEquals(field.getMode(), Field.Mode.NULLABLE);
                    assertEquals(field.getType(), LegacySQLTypeName.DATETIME);
                    break;
                case "timeMillis":
                    assertEquals(field.getMode(), Field.Mode.NULLABLE);
                    assertEquals(field.getType(), LegacySQLTypeName.TIME);
                    break;
                case "timestampMillis":
                    assertEquals(field.getMode(), Field.Mode.NULLABLE);
                    assertEquals(field.getType(), LegacySQLTypeName.TIMESTAMP);
                    break;
                default:
                    fail("Not found match field name, please check Dag record");
            }
        }

    }

    public void assertMap3(FieldList fields) {
        for (Field field : fields) {
            String name = field.getName();
            assertEquals(field.getMode(), Field.Mode.NULLABLE);
            switch (name) {
                case "key":
                    assertEquals(field.getType(), LegacySQLTypeName.STRING);
                    break;
                case "value":
                    assertEquals(field.getType(), LegacySQLTypeName.RECORD);
                    assertDag(field.getSubFields());
                    break;
                default:
                    fail("Map field name must key or value");
            }
        }
    }
}