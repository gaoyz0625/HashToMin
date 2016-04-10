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

package edu.stefano.forti.pad.countnodes;

import edu.stefano.forti.pad.connectedcomponents.JobCounters;
import java.io.IOException;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;

/**
 * This class launches a job to count the number of distinct nodes 
 * and the number of malformed lines in the input file.
 * @author stefano
 */

public class CountNodes extends Configured implements Tool {
    private Path input;    
    private final int reduceTasksNumber;
    private long vertecesStart, malformedLines;
    
    public CountNodes(Path input, int reduceTasksNumber){
        this.input = input;
        this.reduceTasksNumber = reduceTasksNumber;
    }
    
    public long getVertecesStart(){
        return vertecesStart;
    }
    
    public long getMalformedLines(){
        return malformedLines;
    }

    @Override
    public int run(String[] strings) throws IOException, ClassNotFoundException, InterruptedException  {
        
        Job countNodesJob = new Job();

        countNodesJob.setJarByClass(CountNodes.class);
        countNodesJob.setMapperClass(CountNodesMapper.class);
        countNodesJob.setReducerClass(CountNodesReducer.class);
        countNodesJob.setNumReduceTasks(reduceTasksNumber);
        countNodesJob.setMapOutputKeyClass(IntWritable.class);
        countNodesJob.setMapOutputValueClass(NullWritable.class);
        countNodesJob.setOutputKeyClass(NullWritable.class);
        countNodesJob.setOutputValueClass(NullWritable.class);

        FileInputFormat.setInputPaths(countNodesJob, input);
        FileOutputFormat.setOutputPath(countNodesJob, new Path("tmp"));
        countNodesJob.waitForCompletion(false);
        
        vertecesStart = countNodesJob.getCounters().findCounter(JobCounters.VERTECES_START).getValue();
        malformedLines = countNodesJob.getCounters().findCounter(JobCounters.MALFORMED_LINES).getValue();
        return 0;
    }

}
