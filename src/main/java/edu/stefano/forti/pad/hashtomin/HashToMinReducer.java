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
public class HashToMinReducer extends Mapper<LongWritable,Text,Text,IntWritable> {
    @Override
    public void map(LongWritable key, Text value, Context context)
        throws IOException, InterruptedException{
            String line = value.toString();
            String year = line.substring(15, 19);
            int airTemperature;
            if (line.charAt(87) == '+'){
                airTemperature = Integer.parseInt(line.substring(87,92));
                } else { 
                airTemperature = Integer.parseInt(line.substring(92,93));
            }
            
            String quality = line.substring(92,93);
            if (airTemperature != 9999 && quality.matches("[01459]")){
                context.write(new Text(year), new IntWritable(airTemperature));
            }
    }
}
