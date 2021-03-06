package com.longestword;

import com.longestword.map.LongestWordMapper;
import com.longestword.reduce.LongestWordReducer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import static com.sun.activation.registries.LogSupport.log;

public class LongestWord extends Configured implements Tool {

    private static final int ARGS_COUNT = 2;

    public static void main(final String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(), new LongestWord(), args);
        System.exit(res);
    }

    public int run(String[] args) throws Exception {
        Configuration conf = getConf();

        if (args.length != 0 & args.length != ARGS_COUNT) {
            log("Usage: hdfsInputFileOrDirectory hdfsOutputFileOrDirectory");
            return 2;
        }

        String hdfsInputFileOrDirectory = conf.get("dir.input") + "/longestword/input";
        String hdfsOutputFileOrDirectory = conf.get("dir.output") + "/longestword/output";

        if (args.length == ARGS_COUNT) {
            hdfsInputFileOrDirectory = args[0];
            hdfsOutputFileOrDirectory = args[1];
        }

        Job job = Job.getInstance(conf);

        job.setJarByClass(getClass());
        job.setJobName(getClass().getName());

        FileInputFormat.addInputPath(job, new Path(hdfsInputFileOrDirectory));
        FileOutputFormat.setOutputPath(job, new Path(hdfsOutputFileOrDirectory));

        job.setMapperClass(LongestWordMapper.class);
        job.setCombinerClass(LongestWordReducer.class);
        job.setReducerClass(LongestWordReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        return job.waitForCompletion(true) ? 0 : 1;
    }
}
