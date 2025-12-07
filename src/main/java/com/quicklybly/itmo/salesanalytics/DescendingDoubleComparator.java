package com.quicklybly.itmo.salesanalytics;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class DescendingDoubleComparator extends WritableComparator {

    protected DescendingDoubleComparator() {
        super(DoubleWritable.class, true);
    }

    @Override
    public int compare(WritableComparable a, WritableComparable b) {
        DoubleWritable d1 = (DoubleWritable) a;
        DoubleWritable d2 = (DoubleWritable) b;

        // reversed order
        return d2.compareTo(d1);
    }
}
