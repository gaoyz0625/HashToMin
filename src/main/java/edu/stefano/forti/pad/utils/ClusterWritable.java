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
package edu.stefano.forti.pad.utils;

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
