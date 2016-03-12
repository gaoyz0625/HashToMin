/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package edu.stefano.forti.pad.hashtomin;

import java.io.IOException;
import java.util.TreeSet;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 *
 * @author stefano
 */
public class ExportMapper extends Mapper<LongWritable,Text,IntWritable,ClusterWritable> {

    @Override
    public void map(LongWritable key, Text clust, Context context)
        throws IOException, InterruptedException{
        
        String[] verteces = clust.toString().split("[\\s\\t]+");
        
        TreeSet<Integer> tree = new TreeSet<Integer>();
        
        for (String t : verteces){
            tree.add(Integer.parseInt(t));
        }
        
        if ((Integer.parseInt(verteces[0])) == (Integer.parseInt(verteces[1]))){
            context.write(new IntWritable((Integer.parseInt(verteces[0]))), new ClusterWritable(tree) );
        }
       
    } 
}
