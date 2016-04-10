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
package edu.stefano.forti.pad.hashtominsecondarysort;

import edu.stefano.forti.pad.hashtomin.ClusterWritable;
import edu.stefano.forti.pad.connectedcomponents.JobCounters;
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
public class HashToMinSecondarySortReducer extends Reducer<VertexPair, IntWritable, IntWritable, Text> {

    @Override
    public void reduce(VertexPair vertex, Iterable<IntWritable> cluster, Context context)
            throws IOException, InterruptedException {
        
        int currNode = -1, prevNode, length = 1, vMin = -1;
        String result = new String();
        
        Iterator<IntWritable> iterator = cluster.iterator();
        
        prevNode = iterator.next().get();
        vMin = prevNode;
        //removes duplicates via a scan, values have been sorted via secondary sort
        while(iterator.hasNext()){
            currNode = iterator.next().get();
            if (prevNode != currNode){
                result += prevNode + " ";
                prevNode = currNode;
                length++;
            }
        }
        //adds last node
        result += prevNode;
        
        if (vertex.getFirst() > vMin && length > 1 ) //checks the stopping criterion
            context.getCounter(JobCounters.GO_ON).increment(1);
        
        context.write(new IntWritable(vertex.getFirst()),new Text(result));
    }

}
