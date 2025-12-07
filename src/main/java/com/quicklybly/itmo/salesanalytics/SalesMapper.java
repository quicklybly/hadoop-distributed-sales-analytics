package com.quicklybly.itmo.salesanalytics;

import java.io.IOException;
import java.math.BigDecimal;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

// todo pokur org.apache.hadoop.mapreduce.lib.map.MultithreadedMapper
public class SalesMapper extends Mapper<LongWritable, Text, Text, SalesWritable> {

    private final Text categoryKey = new Text();
    private final SalesWritable salesValue = new SalesWritable();

    private static final int IDX_CATEGORY = 2;
    private static final int IDX_PRICE = 3;
    private static final int IDX_QUANTITY = 4;

    private static final String DELIMITER = ",";

    @Override
    protected void map(
            LongWritable key,
            Text value,
            Mapper<LongWritable, Text, Text, SalesWritable>.Context context
    ) throws IOException, InterruptedException {
        String line = value.toString();

        // skip csv header
        if (line.startsWith("transaction_id") || line.trim().isEmpty()) {
            return;
        }

        String[] parts = line.split(DELIMITER);

        if (parts.length < 5) {
            context.getCounter("DATA_QUALITY", "MALFORMED_ROWS").increment(1);
            return;
        }

        try {
            String category = parts[IDX_CATEGORY].trim();
            BigDecimal price = new BigDecimal(parts[IDX_PRICE]);
            int quantity = Integer.parseInt(parts[IDX_QUANTITY]);

            BigDecimal revenue = price.multiply(BigDecimal.valueOf(quantity));

            categoryKey.set(category);
            salesValue.setRevenue(revenue);
            salesValue.setQuantity(quantity);

            context.write(categoryKey, salesValue);
        } catch (NumberFormatException e) {
            context.getCounter("DATA_QUALITY", "PARSE_ERRORS").increment(1);
        }
    }
}
