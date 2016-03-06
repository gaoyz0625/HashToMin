/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package edu.stefano.forti.pad.hashtomin;

import java.io.*;
import java.util.TreeSet;
import org.apache.hadoop.io.*;

/**
 *
 * @author stefano
 */
public class ClusterWritable extends TreeSet<IntWritable> implements Writable{
    
    private static final long serialVersionUID = 1L;
    
    private TreeSet<IntWritable> cluster;
    
    public ClusterWritable(){
        set(new TreeSet<IntWritable>());
    }
    
    public ClusterWritable(TreeSet<IntWritable> cluster){
        set(new TreeSet<IntWritable>());
        set(cluster);
    }
    
    public ClusterWritable(Iterable<IntWritable> cluster){
        set(new TreeSet<IntWritable>());
        for (IntWritable i : cluster){
            this.cluster.add(i);
        }
    }
    
    public void set(TreeSet<IntWritable> cluster){
        this.cluster = cluster;
    }
    
    public TreeSet<IntWritable> get(){
        return cluster;
    }
    
    public void merge(ClusterWritable c){
        TreeSet<IntWritable> tree = c.get();
        
        this.cluster.addAll(c.get());
    }

    @Override
    public void write(DataOutput d) throws IOException {
        int size = cluster.size();
        d.writeInt(size);
        if (size > 0){
            for (IntWritable v : cluster)
                d.writeInt(cluster.pollFirst().get());
        }
    }

    @Override
    public void readFields(DataInput di) throws IOException {
        //throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
        int vertNum = di.readInt();
        if (vertNum > 0 ){
            for (int j = 0; j < vertNum; j++)
                cluster.add(new IntWritable(di.readInt()));
        }
    }
    
    @Override
    public String toString(){
        String result = new String();
        for (IntWritable i : cluster)
            result = result + " " + i.toString();
        return result;
    }

}
