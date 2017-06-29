package com.accesslogs.models;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.DataOutputStream;
import java.io.IOException;

public class CsvOutputFormat extends TextOutputFormat<Text, IpBytesWritable> {

    protected static class CsvRecordWriter extends RecordWriter<Text, IpBytesWritable> {

        private DataOutputStream out;

        CsvRecordWriter(DataOutputStream out) throws IOException {
            this.out = out;
        }

        public synchronized void write(Text key, IpBytesWritable value) throws IOException {
            String result = String.format("%s,%s\n", key.toString(), value.toString());
            out.writeBytes(result);
        }

        public synchronized void close(TaskAttemptContext job)
                throws IOException {
            out.close();
        }
    }

    @Override
    public RecordWriter<Text, IpBytesWritable> getRecordWriter(TaskAttemptContext job)
            throws IOException, InterruptedException {

        String file_extension = ".csv";
        Path file = getDefaultWorkFile(job, file_extension);
        FileSystem fs = file.getFileSystem(job.getConfiguration());
        FSDataOutputStream fileOut = fs.create(file, false);
        return new CsvRecordWriter(fileOut);
    }
}
