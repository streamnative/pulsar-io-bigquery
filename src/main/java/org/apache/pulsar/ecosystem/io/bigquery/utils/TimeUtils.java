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

import java.util.Calendar;
import java.util.TimeZone;
import org.threeten.bp.LocalDateTime;

/**
 * Time utils.
 */
public class TimeUtils {

    // BigQuery uses UTC timezone by default
    private static final TimeZone utcTimeZone = TimeZone.getTimeZone("UTC");

    /**
     * Convert time stamp millis to LocaldateTime.
     *
     * @param timeStampMillis millisecond timestamp
     * @return
     */
    public static LocalDateTime convertLocalDateTime(long timeStampMillis) {
        Calendar calendar = Calendar.getInstance(utcTimeZone);
        calendar.setTimeInMillis(timeStampMillis);
        LocalDateTime localDateTime = LocalDateTime.of(calendar.get(Calendar.YEAR),
                calendar.get(Calendar.MONTH) + 1,
                calendar.get(Calendar.DAY_OF_MONTH), calendar.get(Calendar.HOUR_OF_DAY),
                calendar.get(Calendar.MINUTE), calendar.get(Calendar.SECOND),
                calendar.get(Calendar.MILLISECOND) * 1000000);
        return localDateTime;
    }
}
