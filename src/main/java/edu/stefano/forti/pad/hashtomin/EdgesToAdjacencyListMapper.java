/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package edu.stefano.forti.pad.hashtomin;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
        
    
/**
 *
 * @author stefano
 */
public class EdgesToAdjacencyListMapper extends Mapper<IntWritable,IntWritable,IntWritable,IntWritable> {
    @Override
    public void map(IntWritable vertexFrom, IntWritable vertexTo, Mapper.Context context)
        throws IOException, InterruptedException{
          
            context.write(vertexFrom, vertexTo);
       
    }
}
