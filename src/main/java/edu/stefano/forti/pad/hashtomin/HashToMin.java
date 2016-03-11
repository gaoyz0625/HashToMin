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
        MERGED
    }

    /**
     * @param args the command line arguments
     * @throws java.io.IOException
     * @throws java.lang.InterruptedException
     * @throws java.lang.ClassNotFoundException
     */
    public static void main(String[] args) throws Exception {
        int exitCode = ToolRunner.run(new Configuration(), new HashToMin(), args);
        if (args.length != 2) {
            System.err.println("Usage: <in> <output name> ");
        }
        System.exit(exitCode);

    }

    private Job getJobConf(String[] args) throws Exception {

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
        };

        return setupJob("hashtomin", jobInfo);

    }

    @Override
    public int run(String[] strings) throws Exception {

        Job job;
        long iterate = 1;
        int iterations = 0;
        boolean goOn = true;
        

        while (goOn) {
            job = getJobConf(strings);
            String input, output;

            if (iterations == 0) {
                input = strings[0];
            } else {
                input = strings[1] + iterations;
            }

            output = strings[1] + (iterations + 1);
            FileInputFormat.setInputPaths(job, new Path(input));
            FileOutputFormat.setOutputPath(job, new Path(output));
            job.setMapOutputKeyClass(IntWritable.class);
            job.setMapOutputValueClass(ClusterWritable.class);
            job.waitForCompletion(true);

            Counters counters = job.getCounters();
            goOn = (iterate != counters.findCounter(StopCondition.MERGED).getValue());
            iterate = counters.findCounter(StopCondition.MERGED).getValue();
            counters.findCounter(StopCondition.MERGED).setValue(0);
            iterations++;
        }

        return 0;
    }

}
