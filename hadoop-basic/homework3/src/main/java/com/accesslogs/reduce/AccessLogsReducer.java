package com.accesslogs.reduce;

import com.accesslogs.models.IpBytesWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Iterator;

public final class AccessLogsReducer extends Reducer<Text, LongWritable, Text, IpBytesWritable> {

    @Override
    protected void reduce(Text key, Iterable<LongWritable> values, Context context)
            throws IOException, InterruptedException {

        Iterator itr = values.iterator();

        double avgBytes = 0;
        long totalBytes = 0;

        while (itr.hasNext()) {
            LongWritable bytesWr = (LongWritable) itr.next();
            long bytes = bytesWr.get();

            avgBytes = (avgBytes + bytes) / 2;
            totalBytes += bytes;
        }

        context.write(key, new IpBytesWritable(avgBytes, totalBytes));
    }
}
