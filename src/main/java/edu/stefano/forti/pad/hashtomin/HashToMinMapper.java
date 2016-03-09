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
public class HashToMinMapper extends Mapper<LongWritable,Text,IntWritable,ClusterWritable> {

    @Override
    public void map(LongWritable key, Text couple, Mapper.Context context)
        throws IOException, InterruptedException{
        
        String[] verteces = couple.toString().split("[\\s\\t]+");
        TreeSet<Integer> cluster = new TreeSet();
        
        //builds (v_min, C_v)
        for (String v : verteces) {
            cluster.add(Integer.parseInt(v));         
        }
        
        context.write(new IntWritable(cluster.first()), new ClusterWritable(cluster));
       
        //builds (u, v_min)
        TreeSet<Integer> cTmp = new TreeSet();
        cTmp.add(cluster.first());
        
        for (Integer u : cluster){
            context.write(new IntWritable(u), new ClusterWritable(cTmp));
        }
       
    } 
}
