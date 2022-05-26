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