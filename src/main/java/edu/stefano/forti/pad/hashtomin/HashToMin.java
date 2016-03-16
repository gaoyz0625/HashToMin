/**
The MIT License (MIT)

Copyright (c) 2016 Stefano Forti

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.
**/
package edu.stefano.forti.pad.hashtomin;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.*;

/**
 *
 * @author stefano
 */
public class HashToMin extends BaseJob {

    enum StopCondition {
        GO_ON
    }

    public static void main(String[] args) throws Exception {
        if (args.length == 3) {
            int exitCode = ToolRunner.run(new Configuration(), new HashToMin(), args);
            System.exit(exitCode);
        } else {
            System.out.print("Incorrect use: you should specify input file, output file and number of reduce tasks.");
        }
    }

    @Override
    public int run(String[] strings) throws Exception {

        Job hashToMinJob;
        long iterate = 1;
        int iterations = 0;
        String input, output = null;

        while (iterate > 0) {
            hashToMinJob = getHashToMinJobConf(strings);

            if (iterations == 0) {
                input = strings[0];
            } else {
                input = strings[1] + iterations;
            }

            output = strings[1] + (iterations + 1);
            FileInputFormat.setInputPaths(hashToMinJob, new Path(input));
            FileOutputFormat.setOutputPath(hashToMinJob, new Path(output));

            hashToMinJob.waitForCompletion(true);

            Counters counters = hashToMinJob.getCounters();
            iterate = counters.findCounter(StopCondition.GO_ON).getValue();
            counters.findCounter(StopCondition.GO_ON).setValue(0);
            iterations++;
        }

        Job exportJob;

        exportJob = getExportJobConf(strings);
        FileInputFormat.setInputPaths(exportJob, new Path(output));
        FileOutputFormat.setOutputPath(exportJob, new Path("result"));
        exportJob.waitForCompletion(true);

        return 0;
    }

    private Job getHashToMinJobConf(final String[] args) throws Exception {

        JobInfo jobInfo = new JobInfo() {
            @Override
            public Class<? extends Reducer> getCombinerClass() {
                return null;
            }

            @Override
            public Class<?> getJarByClass() {
                return HashToMin.class;
            }

            @Override
            public Class<? extends Mapper> getMapperClass() {
                return HashToMinMapper.class;
            }

            @Override
            public Class<?> getOutputKeyClass() {
                return IntWritable.class;
            }

            @Override
            public Class<?> getOutputValueClass() {
                return Text.class;
            }

            @Override
            public Class<? extends Reducer> getReducerClass() {
                return HashToMinReducer.class;
            }

            @Override
            public Class<?> getMapOutputKeyClass() {
                return IntWritable.class;
            }

            @Override
            public Class<?> getMapOutputValueClass() {
                return ClusterWritable.class;
            }

            @Override
            public int getNumReduceTasks() {
                return Integer.parseInt(args[2]);
            }
        };

        return setupJob("hashtomin", jobInfo);

    }

    private Job getExportJobConf(String[] args) throws Exception {

        JobInfo jobInfo = new JobInfo() {
            @Override
            public Class<? extends Reducer> getCombinerClass() {
                return null;
            }

            @Override
            public Class<?> getJarByClass() {
                return HashToMin.class;
            }

            @Override
            public Class<? extends Mapper> getMapperClass() {
                return ExportMapper.class;
            }

            @Override
            public Class<?> getOutputKeyClass() {
                return IntWritable.class;
            }

            @Override
            public Class<?> getOutputValueClass() {
                return Text.class;
            }

            @Override
            public Class<? extends Reducer> getReducerClass() {
                return ExportReducer.class;
            }

            @Override
            public Class<?> getMapOutputKeyClass() {
                return IntWritable.class;
            }

            @Override
            public Class<?> getMapOutputValueClass() {
                return ClusterWritable.class;
            }

            @Override
            public int getNumReduceTasks() {
                return 1;
            }
        };

        return setupJob("export", jobInfo);

    }

}
