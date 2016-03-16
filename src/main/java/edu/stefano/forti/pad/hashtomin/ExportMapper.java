/**
The MIT License (MIT)

Copyright (c) 2016 Stefano Forti

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.
**/
package edu.stefano.forti.pad.hashtomin;

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
public class ExportMapper extends Mapper<LongWritable,Text,IntWritable,ClusterWritable> {

    @Override
    public void map(LongWritable key, Text clust, Context context)
        throws IOException, InterruptedException{
        
        String[] verteces = clust.toString().split("[\\s\\t]+");
        
        TreeSet<Integer> tree = new TreeSet<Integer>();
        
        for (String t : verteces){
            tree.add(Integer.parseInt(t));
        }
        
        if ((Integer.parseInt(verteces[0])) == (Integer.parseInt(verteces[1]))){
            context.write(new IntWritable((Integer.parseInt(verteces[0]))), new ClusterWritable(tree) );
        }
       
    } 
}
