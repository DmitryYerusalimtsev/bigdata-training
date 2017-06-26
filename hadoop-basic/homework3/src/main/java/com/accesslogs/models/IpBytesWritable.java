package com.accesslogs.models;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class IpBytesWritable implements Writable {

    private double avgBytes;
    private long totalBytes;

    public IpBytesWritable(double avgBytes, long totalBytes) {
        this.avgBytes = avgBytes;
        this.totalBytes = totalBytes;
    }

    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeDouble(avgBytes);
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
