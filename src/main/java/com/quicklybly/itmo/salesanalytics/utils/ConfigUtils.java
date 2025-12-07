package com.quicklybly.itmo.salesanalytics.utils;

import org.apache.hadoop.conf.Configuration;

public class ConfigUtils {

    private ConfigUtils() {
        throw new IllegalStateException("Utility class");
    }

    public static Configuration getConfiguration() {
        Configuration conf = new Configuration();

        conf.set("mapreduce.framework.name", "yarn");
        conf.set("yarn.resourcemanager.hostname", "resourcemanager");
        conf.set("mapreduce.app-submission.cross-platform", "true");

        conf.set(
                "mapreduce.job.classpath.files",
                "file:///opt/hadoop-3.2.1/share/hadoop/mapreduce/hadoop-mapreduce-client-app-3.2.1.jar"
        );

        return conf;
    }
}
