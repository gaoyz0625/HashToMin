/**
The MIT License (MIT)

Copyright (c) 2016 Stefano Forti

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.
**/
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
