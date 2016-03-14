/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
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

    /**
     * @param args the command line arguments
     * @throws java.io.IOException
     * @throws java.lang.InterruptedException
     * @throws java.lang.ClassNotFoundException
     */
    public static void main(String[] args) throws Exception {
        int exitCode = ToolRunner.run(new Configuration(), new HashToMin(), args);
        System.exit(exitCode);
    }

    private Job getHashToMinJobConf(String[] args) throws Exception {

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
                return 3;
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

}
