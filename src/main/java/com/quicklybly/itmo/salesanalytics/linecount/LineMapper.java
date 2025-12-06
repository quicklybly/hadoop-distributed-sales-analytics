package com.quicklybly.itmo.salesanalytics.linecount;

import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class LineMapper extends Mapper<Object, Text, Text, IntWritable> {

    private final static IntWritable one = new IntWritable(1);
    private final Text word = new Text("total lines");

    @Override
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        context.write(word, one);
    }
}
