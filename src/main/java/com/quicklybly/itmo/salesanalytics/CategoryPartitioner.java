package com.quicklybly.itmo.salesanalytics;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

/**
 * Just for example.
 * Default HashPartitioner works the same.
 *
 * @see org.apache.hadoop.mapreduce.lib.partition.HashPartitioner
 */
public class CategoryPartitioner extends Partitioner<Text, SalesWritable> {

    @Override
    public int getPartition(Text text, SalesWritable salesWritable, int numPartitions) {
        return (text.hashCode() & Integer.MAX_VALUE) % numPartitions;
    }
}
