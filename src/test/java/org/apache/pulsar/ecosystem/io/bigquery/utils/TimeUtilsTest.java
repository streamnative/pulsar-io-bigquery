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