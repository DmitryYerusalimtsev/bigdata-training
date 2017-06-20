package com.longestword.map;

import com.longestword.common.Constants;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.StringTokenizer;

public final class LongestWordMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

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

        context.write(Constants.KEY, new IntWritable(longestWord.getLength()));
    }
}
