package com.quicklybly.itmo.salesanalytics.linecount;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class LineCountDriver {

    public static void main(String[] args) throws Exception {
        // using TextInputFormat with LineRecordReader by default
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "csv line count");

        job.setJarByClass(LineCountDriver.class);

        job.setMapperClass(LineMapper.class);
        job.setCombinerClass(LineReducer.class);
        job.setReducerClass(LineReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        // /input and /output are directories in HDFS
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
