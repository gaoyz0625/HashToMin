/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package edu.stefano.forti.pad.hashtomin;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.*;

enum StopCondition {
    MERGED
}

/**
 *
 * @author stefano
 */
public class HashToMin extends Configured implements Tool {

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
        int exitCode = ToolRunner.run(new Configuration(),new HashToMin(), args);
        System.exit(exitCode);
        
    }

    @Override
    public int run(String[] strings) throws Exception {
        
        Job job = new Job();
        job.setJarByClass(HashToMin.class);
        job.setJobName("To Adjacency Lists");
        job.setMapperClass(HashToMinMapper.class);
        job.setReducerClass(HashToMinReducer.class);
        job.setNumReduceTasks(1);

        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(ClusterWritable.class);

        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(Text.class);
        long iterate = 1;
        int iterations = 0;
        boolean exitCode = false;
        String input, output = null;
     
        
        while (iterate > 0){
            
        if (iterations == 0){
            input = strings[0];
        } else
        {
            input = strings[1]+iterations;
        }
        output = strings[1] + (iterations + 1);
        FileInputFormat.addInputPath(job, new Path(input));
        FileOutputFormat.setOutputPath(job, new Path(output));
        exitCode=job.waitForCompletion(true);
        
        Counters counters = job.getCounters();
        iterate = counters.findCounter(StopCondition.MERGED).getValue();
        counters.findCounter(StopCondition.MERGED).setValue(0);
        iterations++;
        }
   
        
        return exitCode ? 0 : 1;
    }

}
