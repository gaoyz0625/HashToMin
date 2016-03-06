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
import org.apache.hadoop.mapreduce.Reducer;
        
    
/**
 *
 * @author stefano
 */
public class EdgesToAdjacencyListReducer extends Reducer<IntWritable,IntWritable,IntWritable,Text> {

    @Override
    public void reduce(IntWritable vertex, Iterable<IntWritable> neighbours, Context context)
        throws IOException, InterruptedException 
    {
        String tmp = new String();
        tmp = "";
        for (IntWritable i: neighbours){
            tmp = tmp + i.toString() + " ";
        }
        context.write(vertex, new Text(tmp));
    }

}
