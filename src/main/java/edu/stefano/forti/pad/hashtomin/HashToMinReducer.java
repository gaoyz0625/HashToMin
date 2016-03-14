/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package edu.stefano.forti.pad.hashtomin;

import edu.stefano.forti.pad.hashtomin.HashToMin.StopCondition;
import java.io.IOException;
import java.util.TreeSet;

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

        TreeSet<Integer> cluster = new TreeSet<Integer>();
        int v = vertex.get();
        //updates C_v in (v,C_v)
        for (ClusterWritable c : clusters) {   
            cluster.addAll(c.get());
        }
        //checks whether there has been convergence to <u, {v_min}> and <v_min, C> couples
        if (cluster.size() > 1 && v > cluster.first()) {
                context.getCounter(StopCondition.GO_ON).increment(1);
            }
        
        //writes updated (u,C_u) 
        context.write(vertex, new Text(new ClusterWritable(cluster).toString()));
    }

}
