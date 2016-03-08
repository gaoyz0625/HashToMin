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
public class HashToMinMapper extends Mapper<LongWritable,Text,IntWritable,Text> {

    @Override
    public void map(LongWritable key, Text couple, Mapper.Context context)
        throws IOException, InterruptedException{
        
        String[] verteces = couple.toString().split("[\\s\\t]+");
        TreeSet<Integer> cluster = new TreeSet();
        
        //builds (v_min, C_v)
        for (String v : verteces) {
            cluster.add(Integer.parseInt(v));         
        }
        
        Integer vMin = cluster.first();
        context.write(new IntWritable(vMin),new Text(cluster.toString()
                    .replaceAll("\\[", "")
                    .replaceAll("\\]", "")
                    .replaceAll(",", " ")));
       
        //builds (u, v_min)
        TreeSet<Integer> cTmp = new TreeSet();
        cTmp.add(vMin);
        for (Integer u : cluster){
            context.write(new IntWritable(u), new Text(cTmp.toString()
                    .replaceAll("\\[", "")
                    .replaceAll("\\]", "")
                    .replaceAll(",", " ")));
        }
       
    } 
}
