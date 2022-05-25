package org.apache.pulsar.ecosystem.io.bigquery;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import com.google.common.collect.Sets;
import java.io.IOException;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;
import org.apache.pulsar.ecosystem.io.bigquery.exception.BigQueryConnectorRuntimeException;
import org.junit.Test;

/**
 * big query config test.
 */
public class BigQueryConfigTest {


    @Test
    public void testGetDefaultSystemField() {

        BigQueryConfig bigQueryConfig = new BigQueryConfig();
        bigQueryConfig.setClusteredTables(true);
        bigQueryConfig.setPartitionedTables(true);
        bigQueryConfig.setDefaultSystemField("abc, def , ggg, __event_time__");
        Set<String> assertSystemField =
                Sets.newHashSet("abc", "def", "ggg", "__event_time__", "__message_id__");
        assertEquals(new LinkedHashSet<>(assertSystemField),
                new LinkedHashSet<>(bigQueryConfig.getDefaultSystemField()));

        bigQueryConfig.setDefaultSystemField("a a, b b, cc");
        try {
            bigQueryConfig.getDefaultSystemField();
            fail("Should has failed");
        } catch (BigQueryConnectorRuntimeException e) {
        }
    }

    @Test
    public void testLoad() throws IOException {
        Map<String, Object> mapConfig = new HashMap<>();
        mapConfig.put("autoUpdateTable", true);
        mapConfig.put("keyJson", "test-key");

        BigQueryConfig config = BigQueryConfig.load(mapConfig);
        assertTrue(config.isAutoUpdateTable());
        assertEquals(config.getKeyJson(), "test-key");
    }

}