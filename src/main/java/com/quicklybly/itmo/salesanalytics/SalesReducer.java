package com.quicklybly.itmo.salesanalytics;

import java.io.IOException;
import java.math.BigDecimal;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class SalesReducer extends Reducer<Text, SalesWritable, Text, SalesWritable> {

    private final SalesWritable result = new SalesWritable();

    @Override
    protected void reduce(
            Text key,
            Iterable<SalesWritable> values,
            Reducer<Text, SalesWritable, Text, SalesWritable>.Context context
    ) throws IOException, InterruptedException {
        BigDecimal sumRevenue = BigDecimal.ZERO;
        int sumQuantity = 0;

        for (SalesWritable val : values) {
            sumQuantity += val.getQuantity();
            sumRevenue = sumRevenue.add(val.getRevenue());
        }

        result.setRevenue(sumRevenue);
        result.setQuantity(sumQuantity);

        context.write(key, result);
    }
}
