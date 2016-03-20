/*
 * The MIT License
 *
 * Copyright 2016 Stefano Forti
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
 * THE SOFTWARE.
 */
package edu.stefano.forti.pad.hashtomin;

import utils.ClusterWritable;
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
