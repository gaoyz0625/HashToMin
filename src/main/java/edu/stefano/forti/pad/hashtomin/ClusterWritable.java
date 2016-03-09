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
public class ClusterWritable extends TreeSet<Integer> implements Writable {
    
    private TreeSet<Integer> cluster = new TreeSet<Integer>();
    
    public ClusterWritable(){
        super();
    }
    
    public void set(TreeSet<Integer> c){
        cluster = c;
    }
    
    public TreeSet<Integer> get(){
        return cluster;
    }
    
    
    public ClusterWritable(TreeSet<Integer> c){
        cluster = c;
    }
    

    @Override
    public void write(DataOutput d) throws IOException {
        int size = this.size();
        if (size > 0){
            for (Integer i : cluster){
                d.writeInt(i);
            }
        }
    }

    @Override
    public void readFields(DataInput di) throws IOException {
        int size = this.size();
        if (size > 0){
            for (int i = 0; i < size; i++){
                cluster.add(di.readInt());
            }
        }
    }

    
}

