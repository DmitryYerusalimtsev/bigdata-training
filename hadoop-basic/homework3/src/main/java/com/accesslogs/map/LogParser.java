package com.accesslogs.map;

public final class LogParser {
    private final int BYTES_INDEX = 10;

    public Long getBytes(String logString) {
        String[] parts = logString.split(" ");
        return Long.parseLong(parts[BYTES_INDEX]);
    }
}
