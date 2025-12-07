package com.quicklybly.itmo.salesanalytics;

import java.io.IOException;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class SortMapper extends Mapper<LongWritable, Text, DoubleWritable, Text> {

    private final Text outputValue = new Text();
    private final DoubleWritable sortKey = new DoubleWritable();

    @Override
    protected void map(
            LongWritable key,
            Text value,
            Mapper<LongWritable, Text, DoubleWritable, Text>.Context context
    ) throws IOException, InterruptedException {

        String line = value.toString();
        String[] parts = line.split("\t");

        if (parts.length < 3) {
            context.getCounter("DATA_QUALITY", "INVALID_MAPPING_ROWS").increment(1);
            return;
        }

        try {
            // 0 - category, 1 - revenue, 2 - quantity
            double revenueSortValue = Double.parseDouble(parts[1]);
            sortKey.set(revenueSortValue);

            outputValue.set(line);

            context.write(sortKey, outputValue);
        } catch (NumberFormatException e) {
            context.getCounter("DATA_QUALITY", "INVALID_MAPPING_ROWS").increment(1);
        }
    }
}
