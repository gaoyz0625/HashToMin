/*
 * The MIT License
 *
 * Copyright 2016 stefano.
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
package edu.stefano.forti.pad.hashtominsecondarysort;

import edu.stefano.forti.pad.utils.ClusterWritable;
import edu.stefano.forti.pad.utils.JobCounters;
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
class HashToMinSecondarySortMapper extends Mapper<LongWritable,Text,VertexPair,IntWritable> {

    @Override
    public void map(LongWritable key, Text couple, Mapper.Context context)
        throws IOException, InterruptedException{
        
        String[] verteces = couple.toString().split("[\\s\\t]+");
        int vMin = Integer.parseInt(verteces[0]);
        int u;
        
        //builds (v_min, C_v)
        for (int i = 1; i < verteces.length; i++){
            u = Integer.parseInt(verteces[i]);
            if (verteces.length > 2 && u < vMin){
                vMin = u;
            }
            
            context.write(new VertexPair(vMin, u), new IntWritable(u));
        }     

        //builds (u, v_min)
        
        for (int i = 1; i < verteces.length; i++){
            u = Integer.parseInt(verteces[i]);
            context.write(new VertexPair(u, vMin), new IntWritable(vMin));
        }

       
    } 
}
