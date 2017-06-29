package com.accesslogs.map;

import com.accesslogs.counters.AgentCounter;
import eu.bitwalker.useragentutils.Manufacturer;
import eu.bitwalker.useragentutils.UserAgent;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public final class AccessLogsMapper extends Mapper<LongWritable, Text, Text, LongWritable> {
    private static final int INC_STEP = 1;

    private final LogParser parser = new LogParser();

    @Override
    protected void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {

        String request = value.toString();

        parseAgent(request, context);

        String ip = request.substring(0, request.indexOf(" "));
        long bytes = parser.getBytes(request);
        if (bytes != Constants.BAD_INPUT) {
            context.write(new Text(ip), new LongWritable(bytes));
        }
    }

    private void parseAgent(String request, Context context) {
        String userAgentString = request.substring(request.indexOf("\""), request.lastIndexOf("\""));
        UserAgent userAgent = UserAgent.parseUserAgentString(userAgentString);
        Manufacturer man = userAgent.getBrowser().getManufacturer();
        switch (man) {
            case MICROSOFT:
                incCounter(context, AgentCounter.IE);
                break;
            case MOZILLA:
                incCounter(context, AgentCounter.MOZILLA);
                break;
            default:
                incCounter(context, AgentCounter.OTHER);
                break;
        }
    }

    private void incCounter(Context context, AgentCounter counter) {
        context.getCounter(counter).increment(INC_STEP);
    }
}
