/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package edu.stefano.forti.pad.hashtomin;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;


/**
 *
 * @author stefano
 */
public class HashToMinReducer extends Reducer<IntWritable, ClusterWritable, IntWritable, Text> {

    @Override
    public void reduce(IntWritable vertex, Iterable<ClusterWritable> clusters, Context context)
            throws IOException, InterruptedException {
        
        ClusterWritable cluster = new ClusterWritable();
        
        //update C_v in (v,C_v)
        for (ClusterWritable c : clusters) {
            cluster.merge(c);
        }
        
        
        //writes updated (u,C_u) to file
        context.write(vertex, new Text(cluster.toString()));
    }

}
