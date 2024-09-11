package com.viettel.vtnet.traffic.util;

import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;

public class TimeUtils {
    private static final DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy/MM/dd HH:mm:ss.SSS")
            .withZone(ZoneId.systemDefault());

    public static long parseTimestamp(String timeString) {
        try {
            Instant instant = Instant.from(formatter.parse(timeString));
            return instant.toEpochMilli();
        } catch (DateTimeParseException e) {
            throw new IllegalArgumentException("Invalid time format: " + timeString, e);
        }
    }
}