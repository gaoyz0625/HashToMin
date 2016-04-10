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

package edu.stefano.forti.pad.export;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;

/**
 *This class instantiates a job to export those clusters for which the label and
 * the minimum vertex coincide, after running HtM.
 * @author stefano
 */
public class Export extends Configured implements Tool {
    private Path input, output;
    
    public Export(Path input, Path output){
        this.input = input;
        this.output = output;
    }

    @Override
    public int run(String[] strings) throws Exception {
        
        Job exportJob = new Job();

        exportJob.setJarByClass(Export.class);
        exportJob.setMapperClass(ExportMapper.class);
        exportJob.setReducerClass(ExportReducer.class);
        exportJob.setNumReduceTasks(1);
        exportJob.setMapOutputKeyClass(IntWritable.class);
        exportJob.setMapOutputValueClass(Text.class);
        exportJob.setOutputKeyClass(IntWritable.class);
        exportJob.setOutputValueClass(Text.class);

        FileInputFormat.setInputPaths(exportJob, input);
        FileOutputFormat.setOutputPath(exportJob, output);
        exportJob.waitForCompletion(false);

        return 0;
    }

}
