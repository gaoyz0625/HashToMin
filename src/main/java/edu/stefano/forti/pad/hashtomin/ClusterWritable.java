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
public class ClusterWritable implements Writable{
    
    private TreeSet<IntWritable> cluster;
    
    public ClusterWritable(){
        set(new TreeSet<IntWritable>());
    }
    
    public ClusterWritable(TreeSet<IntWritable> cluster){
        set(cluster);
    }
    
    public void set(TreeSet<IntWritable> cluster){
        this.cluster = cluster;
    }
    
    public TreeSet<IntWritable> get(){
        return cluster;
    }

    @Override
    public void write(DataOutput d) throws IOException {
        int size = cluster.size();
        d.writeInt(size);
        if (size > 0){
            for (IntWritable v : cluster)
                cluster.pollFirst().write(d);
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
    
    public String toString(){
        String result = new String();
        for (IntWritable i : cluster)
            result = result + "\\s" + i.toString();
        return result;
    }

}
