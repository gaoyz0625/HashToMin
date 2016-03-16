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

/**
 *
 * @author stefano
 */
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.util.Tool;

public abstract class BaseJob extends Configured implements Tool {

	// method to set the configuration for the job and the mapper and the reducer classes
	protected Job setupJob(String jobName,JobInfo jobInfo) throws Exception {
		Job job = new Job(new Configuration(), jobName);
		job.setJarByClass(jobInfo.getJarByClass());
		job.setMapperClass(jobInfo.getMapperClass());
		if (jobInfo.getCombinerClass() != null)
			job.setCombinerClass(jobInfo.getCombinerClass());
		job.setReducerClass(jobInfo.getReducerClass());
		job.setNumReduceTasks(jobInfo.getNumReduceTasks());
                job.setMapOutputKeyClass(jobInfo.getMapOutputKeyClass());
                job.setMapOutputValueClass(jobInfo.getMapOutputValueClass());
		job.setOutputKeyClass(jobInfo.getOutputKeyClass());
		job.setOutputValueClass(jobInfo.getOutputValueClass());
		
		return job;
	}
	
	protected abstract class JobInfo {
		public abstract Class<?> getJarByClass();
		public abstract Class<? extends Mapper> getMapperClass();
		public abstract Class<? extends Reducer> getCombinerClass();
		public abstract Class<? extends Reducer> getReducerClass();
		public abstract Class<?> getOutputKeyClass();
		public abstract Class<?> getOutputValueClass();
                public abstract Class<?> getMapOutputKeyClass();
                public abstract Class<?> getMapOutputValueClass();
                public abstract int getNumReduceTasks();
		
 	}
}
