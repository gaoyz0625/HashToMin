/*
 * The MIT License
 *
 * Copyright 2016 stefano.
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
package edu.stefano.forti.pad.hashtominsecondarysort;

import java.io.*;

import org.apache.hadoop.io.*;

public class VertexPair implements WritableComparable<VertexPair> {

  private int first;
  private int second;
  
  public VertexPair() {
  }
  
  public VertexPair(int first, int second) {
    set(first, second);
  }
  
  public void set(int first, int second) {
    this.first = first;
    this.second = second;
  }
  
  public int getFirst() {
    return first;
  }

  public int getSecond() {
    return second;
  }

  @Override
  public void write(DataOutput out) throws IOException {
    out.writeInt(first);
    out.writeInt(second);
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    first = in.readInt();
    second = in.readInt();
  }
  
  @Override
  public int hashCode() {
    return first * 163 + second;
  }
  
  @Override
  public boolean equals(Object o) {
    if (o instanceof VertexPair) {
      VertexPair ip = (VertexPair) o;
      return first == ip.first && second == ip.second;
    }
    return false;
  }

  @Override
  public String toString() {
    return first + "\t" + second;
  }
  
  @Override
  public int compareTo(VertexPair ip) {
    int cmp = compare(first, ip.first);
    if (cmp != 0) {
      return cmp;
    }
    return compare(second, ip.second);
  }
  
  /**
   * Method for comparing two ints.
     * @param a
     * @param b
   */
  public static int compare(int a, int b) {
    return (a < b ? -1 : (a == b ? 0 : 1));
  }
  
}


