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

        IntWritable result = new IntWritable(0);
        String stringValue = value.toString();

        if (stringValue != null && !stringValue.trim().isEmpty()) {

            StringTokenizer itr = new StringTokenizer(stringValue, " ");
            Text longestWord = new Text(itr.nextToken());

            while (itr.hasMoreTokens()) {
                String token = itr.nextToken();
                int length = token.length();
                if (longestWord.getLength() < length) {
                    longestWord.set(token);
                }
            }
            result.set(longestWord.getLength());
        }

        context.write(Constants.KEY, result);
    }
}
