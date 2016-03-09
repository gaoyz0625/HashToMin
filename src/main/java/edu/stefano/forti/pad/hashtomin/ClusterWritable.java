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

    private static final long serialVersionUID = 1L;

    private TreeSet<Integer> cluster = new TreeSet<Integer>();

    public ClusterWritable() {
        super();
    }

    public ClusterWritable(TreeSet<Integer> c) {
        set(c);
    }

    private void set(TreeSet<Integer> c) {
        this.cluster = c;
    }

    public TreeSet<Integer> get() {
        return cluster;
    }

    @Override
    public void write(DataOutput d) throws IOException {
        int size = cluster.size();
        d.writeInt(size);

        for (Integer i : cluster) {
            d.writeInt(i);
        }
    }

    @Override
    public void readFields(DataInput di) throws IOException {
        if (this.cluster != null) {
            this.cluster.clear();
        } else {
            this.cluster = new TreeSet<Integer>();
        }

        int size = di.readInt();
        if (size > 0) {
            for (int i = 0; i < size; i++) {
                cluster.add(di.readInt());
            }
        }
    }

    @Override
    public String toString() {
        return cluster.toString().replaceAll("\\[", "")
                .replaceAll("\\]", "")
                .replaceAll(",", " ");
    }
    
     
 @Override
 public int hashCode() {
 
  int result = 0;
  result = this.cluster.hashCode();
  return result;
 }
 


}
