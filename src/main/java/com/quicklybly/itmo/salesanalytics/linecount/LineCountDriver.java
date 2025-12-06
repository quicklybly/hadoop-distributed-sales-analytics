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

        conf.set("mapreduce.framework.name", "yarn");
        conf.set("yarn.resourcemanager.hostname", "resourcemanager");
        conf.set("mapreduce.app-submission.cross-platform", "true");

        conf.set(
            "mapreduce.job.classpath.files",
            "file:///opt/hadoop-3.2.1/share/hadoop/mapreduce/hadoop-mapreduce-client-app-3.2.1.jar"
        );

        conf.set("yarn.app.mapreduce.am.resource.mb", "512");
        conf.set("mapreduce.map.memory.mb", "512");
        conf.set("mapreduce.reduce.memory.mb", "512");

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
