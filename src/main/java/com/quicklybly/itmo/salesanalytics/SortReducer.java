package com.quicklybly.itmo.salesanalytics;

import java.io.IOException;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class SortReducer extends Reducer<DoubleWritable, Text, Text, Text> {

    private boolean headerWritten = false;
    private final Text finalKey = new Text();
    private final Text finalValue = new Text();

    @Override
    protected void reduce(
            DoubleWritable key,
            Iterable<Text> values,
            Reducer<DoubleWritable, Text, Text, Text>.Context context
    ) throws IOException, InterruptedException {

        if (!headerWritten) {
            context.write(new Text("Category"), new Text("Revenue\tQuantity"));
            headerWritten = true;
        }

        for (Text val : values) {
            // 0 - category, 1 - revenue, 2 - quantity
            String[] parts = val.toString().split("\t");

            String category = parts[0];
            String revenue = parts[1];
            String quantity = parts[2];

            finalKey.set(category);
            finalValue.set(revenue + "\t" + quantity);

            context.write(finalKey, finalValue);
        }
    }
}
