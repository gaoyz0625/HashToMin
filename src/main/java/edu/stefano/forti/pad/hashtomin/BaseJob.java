/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
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
