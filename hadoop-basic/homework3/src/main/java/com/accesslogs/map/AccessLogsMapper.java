package com.accesslogs.map;

import eu.bitwalker.useragentutils.UserAgent;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class AccessLogsMapper extends Mapper<LongWritable, Text, Text, LongWritable> {

    @Override
    protected void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {

        String request = value.toString();

        String ip = request.substring(0, request.indexOf(" "));
        String userAgentString = request.substring(request.indexOf("\""), request.lastIndexOf("\""));

        UserAgent userAgent = UserAgent.parseUserAgentString(userAgentString);
        LongWritable bytes = new LongWritable(userAgent.get)

        context.write(new Text(ip), );
    }
}
