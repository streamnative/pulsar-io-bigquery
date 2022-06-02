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
package org.apache.pulsar.ecosystem.io.bigquery.utils;

import static org.junit.Assert.assertTrue;
import com.google.cloud.bigquery.storage.v1.CivilTimeEncoder;
import org.junit.Test;
import org.threeten.bp.LocalDateTime;

/**
 * time utils test.
 */
public class TimeUtilsTest {

    @Test
    public void convertLocalDateTime() {
        long timeStamp = 1653465394842L;
        LocalDateTime localDateTime = TimeUtils.convertLocalDateTime(timeStamp);
        long datetimeMicros = CivilTimeEncoder.encodePacked64DatetimeMicros(localDateTime);
        assertTrue(datetimeMicros == 142311060792989968L);
    }
}