/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package edu.stefano.forti.pad.hashtomin;

import java.io.IOException;
import java.util.TreeSet;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;


/**
 *
 * @author stefano
 */
public class HashToMinReducer extends Reducer<IntWritable, Text, IntWritable, Text> {

    @Override
    public void reduce(IntWritable vertex, Iterable<Text> clusters, Context context)
            throws IOException, InterruptedException {
        
        TreeSet<Integer> cluster = new TreeSet();
        
        //update C_v in (v,C_v)
        for (Text c : clusters) {
            String[] verteces = c.toString().split("[\\s\\t]+");
            for (String v : verteces){
                cluster.add(Integer.parseInt(v));
            }
        }

        //writes updated (u,C_u) to file
        context.write(vertex,new Text(cluster.toString()
                    .replaceAll("\\[", "")
                    .replaceAll("\\]", "")
                    .replaceAll(",", " ")));
    }

}
