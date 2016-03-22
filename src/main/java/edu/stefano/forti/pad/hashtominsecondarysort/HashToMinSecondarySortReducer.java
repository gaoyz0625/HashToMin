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
import java.util.Iterator;
import java.util.TreeSet;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 *
 * @author stefano
 */
class HashToMinSecondarySortReducer extends Reducer<VertexPair, IntWritable, VertexPair, Text> {

    @Override
    public void reduce(VertexPair vertex, Iterable<IntWritable> cluster, Context context)
            throws IOException, InterruptedException {
        
        int currNode, tmpNode = -1, length = 0;
        String result = new String();
        
        Iterator<IntWritable> iterator = cluster.iterator();
        
        while(iterator.hasNext()){
            currNode = iterator.next().get();
            while (iterator.hasNext() && (tmpNode = iterator.next().get())== currNode){    
                length++;
            }
            result += currNode + " ";
            currNode = tmpNode;
            length++;
        }
        
        //if (vertex.getFirst() > vertex.getSecond() && length > 1 )
            context.getCounter(JobCounters.GO_ON).increment(1);
         context.write(vertex,new Text(result));
    }

}
