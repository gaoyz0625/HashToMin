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
public class EdgesToAdjacencyListMapper extends Mapper<LongWritable,Text,LongWritable,ClusterWritable> {
    @Override
    public void map(LongWritable vertexFrom, Text vertexTo, Mapper.Context context)
        throws IOException, InterruptedException{
        
        TreeSet<LongWritable> t = new TreeSet<LongWritable>();
        t.add(new LongWritable(Integer.parseInt(vertexTo.toString())));
          
            context.write(vertexFrom, new ClusterWritable(t));
       
    }
}
