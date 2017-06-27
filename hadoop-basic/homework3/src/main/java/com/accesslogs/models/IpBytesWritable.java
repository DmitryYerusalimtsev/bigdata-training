package com.accesslogs.models;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class IpBytesWritable implements Writable {

    private DoubleWritable avgBytes;
    private LongWritable totalBytes;

    public IpBytesWritable(double avgBytes, long totalBytes) {
        this.avgBytes = new DoubleWritable(avgBytes);
        this.totalBytes = new LongWritable(totalBytes);
    }

    public void write(DataOutput dataOutput) throws IOException {
        avgBytes.write(dataOutput);
        totalBytes.write(dataOutput);
    }

    public void readFields(DataInput dataInput) throws IOException {
        avgBytes.readFields(dataInput);
        totalBytes.readFields(dataInput);
    }

    @Override
    public String toString() {
        return String.format("%s,%d", avgBytes.get(), totalBytes.get());
    }
}
