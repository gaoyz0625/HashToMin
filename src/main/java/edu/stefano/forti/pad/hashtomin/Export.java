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
public class Export extends BaseJob {

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

        String input, output = null;
        Job exportJob;

        exportJob = getExportJobConf(strings);
        FileInputFormat.setInputPaths(exportJob, new Path(output));
        FileOutputFormat.setOutputPath(exportJob, new Path("result"));
        exportJob.waitForCompletion(true);

        return 0;
    }

    private Job getExportJobConf(String[] args) throws Exception {

        BaseJob.JobInfo jobInfo = new BaseJob.JobInfo() {
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
