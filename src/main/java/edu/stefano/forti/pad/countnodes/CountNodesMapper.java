/*
 * The MIT License
 *
 * Copyright 2016 Stefano Forti.
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
package edu.stefano.forti.pad.countnodes;

import edu.stefano.forti.pad.connectedcomponents.JobCounters;
import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 *
 * @author stefano
 */
public class CountNodesMapper extends Mapper<LongWritable,Text,IntWritable,NullWritable> {
    /**
     * For each vertex identifier v in a line emits <v, null>; if a line is malformed 
     * increments a counter.
     * @param key
     * @param line
     * @param context
     * @throws IOException
     * @throws InterruptedException 
     */
    @Override
    public void map(LongWritable key, Text line, Context context)
        throws IOException, InterruptedException{ 
        NullWritable nw = NullWritable.get();
        
        if (line.toString().matches("[0-9\\s\\t]+")) {
            String[] verteces = line.toString().split("[\\s\\t]+");
            for (String vertex : verteces) {
                context.write(new IntWritable(Integer.parseInt(vertex)), nw);
            }
        } else {
            context.getCounter(JobCounters.MALFORMED_LINES).increment(1);
        }
    } 
}
