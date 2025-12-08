package com.quicklybly.itmo.salesanalytics;

import com.quicklybly.itmo.salesanalytics.utils.ConfigUtils;
import java.io.IOException;
import java.util.UUID;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.NLineInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class SalesDriver {

    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        Configuration conf = ConfigUtils.getConfiguration();

        Job aggregationJob = Job.getInstance(conf, "sales analysis: aggregation");
        aggregationJob.setJarByClass(SalesDriver.class);

        int numberOfLines = 3_000_000;
        if (args.length > 2) {
            numberOfLines = Integer.parseInt(args[2]);
        }
        aggregationJob.setInputFormatClass(NLineInputFormat.class);
        NLineInputFormat.setNumLinesPerSplit(aggregationJob, numberOfLines);

        aggregationJob.setMapperClass(SalesMapper.class);

        boolean useCombiner = true;
        if (args.length > 4) {
            useCombiner = Boolean.parseBoolean(args[4]);
        }
        if (useCombiner) {
            aggregationJob.setCombinerClass(SalesReducer.class);
        }

        aggregationJob.setReducerClass(SalesReducer.class);

        int numberOfReduceTasks = 1;
        if (args.length > 3) {
            numberOfReduceTasks = Integer.parseInt(args[3]);
        }
        aggregationJob.setNumReduceTasks(numberOfReduceTasks);
        aggregationJob.setPartitionerClass(CategoryPartitioner.class);

        aggregationJob.setMapOutputKeyClass(Text.class);
        aggregationJob.setMapOutputValueClass(SalesWritable.class);

        aggregationJob.setOutputKeyClass(Text.class);
        aggregationJob.setOutputValueClass(SalesWritable.class);

        String tempAggregationName = "/temp_aggr" + UUID.randomUUID();
        System.out.println("tempAggregationName: " + tempAggregationName);

        FileInputFormat.addInputPath(aggregationJob, new Path(args[0]));
        FileOutputFormat.setOutputPath(aggregationJob, new Path(tempAggregationName));

        if (!aggregationJob.waitForCompletion(true)) {
            System.exit(1);
        }

        Job sortingJob = Job.getInstance(conf, "sales analysis: sorting");
        sortingJob.setJarByClass(SalesDriver.class);

        sortingJob.setMapperClass(SortMapper.class);
        sortingJob.setReducerClass(SortReducer.class);

        sortingJob.setMapOutputValueClass(Text.class);
        sortingJob.setMapOutputKeyClass(DoubleWritable.class);

        sortingJob.setSortComparatorClass(DescendingDoubleComparator.class);

        // using one reducer to sort all data
        sortingJob.setNumReduceTasks(1);

        FileInputFormat.addInputPath(sortingJob, new Path(tempAggregationName));
        FileOutputFormat.setOutputPath(sortingJob, new Path(args[1]));

        boolean success = sortingJob.waitForCompletion(true);

        System.exit(success ? 0 : 1);
    }
}
