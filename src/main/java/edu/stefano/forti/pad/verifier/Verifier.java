/*
 * The MIT License
 *
 * Copyright 2016 Stefano Forti.
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

package edu.stefano.forti.pad.verifier;

import edu.stefano.forti.pad.connectedcomponents.JobCounters;
import java.io.IOException;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.*;

/**
 * It represents the output verifier tasks, must run after HtM.
 * @author stefano
 */
public class Verifier extends Configured implements Tool{
    
    private final Path input;
    private long vertecesEnd, duplicates;
    private final int reduceTasksNumber;
    
    public Verifier (Path input, int reduceTasksNumber){
        this.input = input;
        this.reduceTasksNumber = reduceTasksNumber;
    }
    
    public long getDuplicates(){
        return duplicates;
    }
    
    public long getVertecesEnd(){
        return vertecesEnd;
    }

    @Override
    public int run(String[] strings) throws IOException, InterruptedException, ClassNotFoundException {

        Job verifierJob = new Job();
        
        verifierJob.setJarByClass(Verifier.class);
        verifierJob.setMapperClass(VerifierMapper.class);
        verifierJob.setReducerClass(VerifierReducer.class);
        verifierJob.setNumReduceTasks(reduceTasksNumber);
        verifierJob.setMapOutputKeyClass(IntWritable.class);
        verifierJob.setMapOutputValueClass(NullWritable.class);
        verifierJob.setOutputKeyClass(NullWritable.class);
        verifierJob.setOutputValueClass(NullWritable.class);

        FileInputFormat.setInputPaths(verifierJob, input);
        FileOutputFormat.setOutputPath(verifierJob, new Path ("tmp"));
        verifierJob.waitForCompletion(false);
        
        duplicates = verifierJob.getCounters().findCounter(JobCounters.DUPLICATES).getValue();
        vertecesEnd = verifierJob.getCounters().findCounter(JobCounters.VERTECES_END).getValue();

        
        return 0;
    }

}
