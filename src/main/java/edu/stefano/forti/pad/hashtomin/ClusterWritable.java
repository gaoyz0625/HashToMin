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
public class ClusterWritable extends TreeSet<IntWritable> implements Writable {
    
    private static final long serialVersionUID = 1L;
    
    private TreeSet<IntWritable> cluster;
    
    public ClusterWritable(){
        set(new TreeSet<IntWritable>());
    }
    
    public ClusterWritable(TreeSet<IntWritable> cluster){
        set(cluster);
    }
    
    public ClusterWritable(Iterable<IntWritable> cluster){
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
        IntWritable size = new IntWritable(cluster.size());
        size.write(d);
        if (size.get() > 0){
            for (IntWritable v : cluster)
                v.write(d);
        }
    }

    @Override
    public void readFields(DataInput di) throws IOException{
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
    
    @Override
    public int hashCode() {
        int result = cluster.hashCode();
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null) {
            return false;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }
        final ClusterWritable other = (ClusterWritable) obj;
        if (this.cluster != other.cluster && (this.cluster == null || !this.cluster.equals(other.cluster))) {
            return false;
        }
        return true;
    }


}
