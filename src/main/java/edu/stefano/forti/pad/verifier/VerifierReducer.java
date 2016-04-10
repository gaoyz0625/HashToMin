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
package edu.stefano.forti.pad.verifier;

import edu.stefano.forti.pad.connectedcomponents.JobCounters;
import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 *
 * @author stefano
 */
public class VerifierReducer extends Reducer<IntWritable, NullWritable, NullWritable, NullWritable> {
    /**
     * Increment a counter when duplicates are found.
     * @param vertex
     * @param occurrences
     * @param context
     * @throws IOException
     * @throws InterruptedException 
     */
    @Override
    public void reduce(IntWritable vertex, Iterable<NullWritable> occurrences, Context context)
            throws IOException, InterruptedException {
        int copies = 0;

        context.getCounter(JobCounters.VERTECES_END).increment(1);
        //counts duplicates
        for (NullWritable occ : occurrences) {
            copies++;
        }
        //are ther duplicates? increment the counter!
        if (copies > 1) {
            context.getCounter(JobCounters.DUPLICATES).increment(copies-1);
        }

    }

}
