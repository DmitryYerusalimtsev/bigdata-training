package com.longestword.map;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.StringTokenizer;

public final class LongestWordMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

    static final Text KEY = new Text("Longest");

    @Override
    protected void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {

        StringTokenizer itr = new StringTokenizer(value.toString());
        Text longestWord = new Text(itr.nextToken());

        while (itr.hasMoreTokens()) {
            int length = itr.nextToken().length();
            if (longestWord.getLength() < length) {
                longestWord.set(value);
            }
        }

        context.write(KEY, new IntWritable(longestWord.getLength()));
    }
}
