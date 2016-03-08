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
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

/**
 *
 * @author stefano
 */
public class HashToMinMapper extends Mapper<LongWritable,Text,IntWritable,ClusterWritable> {

    @Override
    public void map(LongWritable key, Text couple, Mapper.Context context)
        throws IOException, InterruptedException{
        
        String[] verteces = couple.toString().split("[\\s\\t]+");
        ClusterWritable cluster = new ClusterWritable();
        
        //builds (v_min, C_v)
        for (String v : verteces) {
            cluster.add(new IntWritable(Integer.parseInt(v)));         
        }
        
        IntWritable vMin = cluster.first();
        context.write(vMin,cluster);
       
        //builds (u, v_min)
        ClusterWritable cTmp = new ClusterWritable();
        cTmp.add(vMin);
        for (IntWritable u : cluster){
            context.write(u, cTmp);
        }
       
    } 
}
