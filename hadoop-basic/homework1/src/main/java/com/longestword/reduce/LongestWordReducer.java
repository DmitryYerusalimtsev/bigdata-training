package com.longestword.reduce;

import com.longestword.common.Constants;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Iterator;

public class LongestWordReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context)
            throws IOException, InterruptedException {

        Iterator itr = values.iterator();
        IntWritable maxLength = (IntWritable) itr.next();

        while (itr.hasNext()) {
            IntWritable length = (IntWritable) itr.next();
            if (maxLength.compareTo(length) < 0) {
                maxLength = length;
            }
        }

        context.write(Constants.KEY, maxLength);
    }
}
