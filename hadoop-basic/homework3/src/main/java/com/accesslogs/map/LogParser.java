package com.accesslogs.map;

public final class LogParser {
    private final int BYTES_INDEX = 9;

    public long getBytes(String logString) {
        try {
            String[] parts = logString.split(" ");
            return Long.parseLong(parts[BYTES_INDEX]);
        } catch (NumberFormatException e) {
            return Constants.BAD_INPUT;
        }
    }
}
