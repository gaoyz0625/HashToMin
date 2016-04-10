/*
 * The MIT License
 *
 * Copyright 2016 stefano.
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
package edu.stefano.forti.pad.connectedcomponents;

import edu.stefano.forti.pad.hashtomin.ClusterWritable;
import edu.stefano.forti.pad.hashtomin.HashToMin;
import edu.stefano.forti.pad.hashtomin.HashToMinMapper;
import edu.stefano.forti.pad.hashtomin.HashToMinReducer;
import java.io.IOException;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;

/**
 *
 * @author stefano
 */
public abstract class HtM extends Configured implements Tool {
    protected Path input, output;
    protected int reduceTasksNumber;

    public HtM(Path input, Path output, int reduceTasksNumber) {
        this.input = input;
        this.output = output;
        this.reduceTasksNumber = reduceTasksNumber;
    }
    
    /**
     * Sets up the HtM Job.
     * @return
     */
    protected abstract Job setupJob() throws IOException;
    
    public int run(String[] args) throws IOException, InterruptedException, ClassNotFoundException {

        Job hashToMinJob = setupJob();
 
        long iterate;
        int result = -1;

        FileInputFormat.setInputPaths(hashToMinJob, input);
        FileOutputFormat.setOutputPath(hashToMinJob, output);

        hashToMinJob.waitForCompletion(true);
        //retrieve the GO_ON counter, if > 0 must return a positive value
        Counters counters = hashToMinJob.getCounters();
        iterate = counters.findCounter(JobCounters.GO_ON).getValue();
        counters.findCounter(JobCounters.GO_ON).setValue(0);

        if (iterate > 0) {
            result = 1;
        } else {
            result = 0;
        }

        return result;
    }
}
