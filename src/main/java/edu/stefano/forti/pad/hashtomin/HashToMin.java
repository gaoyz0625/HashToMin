/*
 * The MIT License
 *
 * Copyright 2016 Stefano Forti
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
 * THE SOFTWARE.
 */
package edu.stefano.forti.pad.hashtomin;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 *
 * @author stefano
 */
public class HashToMin extends BaseJob {

    private Path input, output;
    private final int reduceTasksNumber;

    public HashToMin(Path input, Path output, int reduceTasksNumber) {
        this.input = input;
        this.output = output;
        this.reduceTasksNumber = reduceTasksNumber;
    }

    @Override
    public int run(String[] args) throws Exception {
        
        Job hashToMinJob;
        String[] strings = new String[1];
        strings[0] = Integer.toString(reduceTasksNumber);
        long iterate;
        int result = -1;

        hashToMinJob = getHashToMinJobConf(strings);

        FileInputFormat.setInputPaths(hashToMinJob, input);
        FileOutputFormat.setOutputPath(hashToMinJob, output);

        hashToMinJob.waitForCompletion(true);
        
        Counters counters = hashToMinJob.getCounters();
        iterate = counters.findCounter(JobCounters.GO_ON).getValue();
        counters.findCounter(JobCounters.GO_ON).setValue(0);
        
        if (iterate > 0)
            result = 1;
        else
            result = 0;

        return result;
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
                return Integer.parseInt(args[0]);
            }
        };

        return setupJob("hashtomin", jobInfo);

    }

    public static void main(String[] args) throws Exception {
        if (args.length == 3) {
            //int exitCode = ToolRunner.run(new Configuration(), new HashToMin(input, output, reduceTasksNumber), args);
//            System.exit(exitCode);
        } else {
            System.out.print("Incorrect use: you should specify input file, output file and number of reduce tasks.");
        }
    }

}
