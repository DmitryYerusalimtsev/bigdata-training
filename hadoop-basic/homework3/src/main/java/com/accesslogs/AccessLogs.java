package com.accesslogs;

import com.accesslogs.map.AccessLogsMapper;
import com.accesslogs.models.IpBytesWritable;
import com.accesslogs.reduce.AccessLogsReducer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import static com.sun.activation.registries.LogSupport.log;

public final class AccessLogs extends Configured implements Tool {

    private static final int ARGS_COUNT = 2;

    public static void main(final String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(), new AccessLogs(), args);
        System.exit(res);
    }

    public int run(String[] args) throws Exception {
        Configuration conf = getConf();

        if (args.length != 0 & args.length != ARGS_COUNT) {
            log("Usage: hdfsInputFileOrDirectory hdfsOutputFileOrDirectory");
            return 2;
        }

        String hdfsInputFileOrDirectory = conf.get("dir.input") + "/accesslogs/input";
        String hdfsOutputFileOrDirectory = conf.get("dir.output") + "/accesslogs/output";

        if (args.length == ARGS_COUNT) {
            hdfsInputFileOrDirectory = args[0];
            hdfsOutputFileOrDirectory = args[1];
        }

        // Enable compression for results using Snappy.
        conf.set("mapreduce.map.output.compress", "true");
        conf.set("mapred.map.output.compress.codec", "org.apache.hadoop.io.compress.SnappyCodec");

        Job job = Job.getInstance(conf);

        job.setJarByClass(getClass());
        job.setJobName(getClass().getName());

        FileInputFormat.addInputPath(job, new Path(hdfsInputFileOrDirectory));
        FileOutputFormat.setOutputPath(job, new Path(hdfsOutputFileOrDirectory));

        job.setInputFormatClass(TextInputFormat.class);
        //job.setOutputFormatClass(CsvOutputFormat.class);
        job.setOutputFormatClass(SequenceFileOutputFormat.class);

        job.setMapperClass(AccessLogsMapper.class);
        //job.setCombinerClass(AccessLogsReducer.class);
        job.setReducerClass(AccessLogsReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(LongWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IpBytesWritable.class);

        return job.waitForCompletion(true) ? 0 : 1;
    }
}
