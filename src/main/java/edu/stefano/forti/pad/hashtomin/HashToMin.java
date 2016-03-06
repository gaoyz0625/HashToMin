/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package edu.stefano.forti.pad.hashtomin;

import java.io.IOException;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 *
 * @author stefano
 */
public class HashToMin {

    /**
     * @param args the command line arguments
     * @throws java.io.IOException
     * @throws java.lang.InterruptedException
     * @throws java.lang.ClassNotFoundException
     */
    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        
//        Job job = new Job();
//        job.setJarByClass(HashToMin.class);
//        job.setJobName("To Adjacency Lists");
//        
//        FileInputFormat.addInputPath(job, new Path(args[0]));
//        FileOutputFormat.setOutputPath(job, new Path(args[1]));
//        
//        job.setMapperClass(EdgesToAdjacencyListMapper.class);
//        job.setReducerClass(EdgesToAdjacencyListReducer.class);
//        job.setNumReduceTasks(1);
//        
//        job.setMapOutputKeyClass(IntWritable.class);
//        job.setMapOutputValueClass(IntWritable.class);
//
//        job.setOutputKeyClass(IntWritable.class);
//        job.setOutputValueClass(Text.class);
//        
//        System.exit(job.waitForCompletion(true)? 0 : 1);

        Job job = new Job();
        job.setJarByClass(HashToMin.class);
        job.setJobName("To Adjacency Lists");
        
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        
        job.setMapperClass(HashToMinMapper.class);
        job.setReducerClass(HashToMinReducer.class);
        job.setNumReduceTasks(1);
        
        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(ClusterWritable.class);

        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(Text.class);
        
        System.exit(job.waitForCompletion(true)? 0 : 1);
        
    }
    
}
