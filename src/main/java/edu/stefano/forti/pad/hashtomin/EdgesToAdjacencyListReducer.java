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
import org.apache.hadoop.mapreduce.Reducer;
        
    
/**
 *
 * @author stefano
 */
public class EdgesToAdjacencyListReducer extends Reducer<LongWritable,ClusterWritable,LongWritable,Text> {

    @Override
    public void reduce(LongWritable vertex, Iterable<ClusterWritable> cluster, Context context)
        throws IOException, InterruptedException 
    {
        Text t = new Text (" " + (cluster.toString()));
        context.write(vertex, t );
    }

}
