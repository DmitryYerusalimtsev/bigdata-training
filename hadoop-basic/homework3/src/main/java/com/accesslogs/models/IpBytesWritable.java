package com.accesslogs.models;

import eu.bitwalker.useragentutils.UserAgent;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class IpBytesWritable implements Writable {

    private int avgBytes;
    private long totalBytes;

    public IpBytesWritable(UserAgent userAgent)

    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeInt(avgBytes);
        dataOutput.writeLong(totalBytes);
    }

    public void readFields(DataInput dataInput) throws IOException {
        avgBytes = dataInput.readInt();
        totalBytes = dataInput.readLong();
    }

    @Override
    public String toString() {
        return String.format("%d,%d", avgBytes, totalBytes);
    }
}
