/*
 * The MIT License
 *
 * Copyright 2016 Stefano Forti.
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


import edu.stefano.forti.pad.hashtominsecondarysort.*;
import edu.stefano.forti.pad.hashtominsecondarysort.VertexPair;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.apache.hadoop.mrunit.types.Pair;
import org.junit.Before;
import org.junit.Test;

/**
 *
 * @author stefano
 */
public class MapReducerTester {
  static MapDriver<LongWritable, Text, VertexPair, IntWritable> mapDriver;
  static ReduceDriver<VertexPair, IntWritable, IntWritable, Text> reduceDriver;
  static MapReduceDriver<LongWritable, Text, VertexPair, IntWritable, IntWritable, Text> mapReduceDriver;
 
  @Before
  public void setUp() {
    HashToMinSecondarySortMapper mapper = new HashToMinSecondarySortMapper();
    HashToMinSecondarySortReducer reducer = new HashToMinSecondarySortReducer();
    mapDriver = MapDriver.newMapDriver(mapper);
    reduceDriver = ReduceDriver.newReduceDriver(reducer);
    mapReduceDriver = MapReduceDriver.newMapReduceDriver(mapper, reducer);
  }
 
  @Test
  public void testMapper() throws IOException {

//    mapDriver.addInput(new LongWritable(2), new Text("5\t3 4 5"));
    
    mapDriver.withInput(new LongWritable(1), new Text("1\t1 2 4"))
            .withOutput(new VertexPair(1,1), new IntWritable(1))
            .withOutput(new VertexPair(1,1), new IntWritable(1))
            .withOutput(new VertexPair(1,1), new IntWritable(1))
            .withOutput(new VertexPair(1,1), new IntWritable(1))
            .withOutput(new VertexPair(1,2), new IntWritable(2))
            .withOutput(new VertexPair(2,1), new IntWritable(1))
            .withOutput(new VertexPair(1,4), new IntWritable(4))
            .withOutput(new VertexPair(4,1), new IntWritable(1));
    mapDriver.runTest();
  }
 
  @Test
  public void testReducer() throws IOException {
    List<IntWritable> values = new ArrayList<IntWritable>();
    values.add(new IntWritable(1));
    values.add(new IntWritable(1));
    values.add(new IntWritable(2));

    reduceDriver.withInput(new VertexPair(1,1), values);
    reduceDriver.withOutput(new IntWritable(1), new Text("1 2") );
    reduceDriver.runTest();
  }
  
   @Test

  public void testMapReduce() throws IOException {
      
    List<Pair<LongWritable,Text>> inputs = new ArrayList<Pair<LongWritable,Text>>();
    inputs.add(new Pair(new LongWritable(), new Text("1\t2 3 4 5")));
    inputs.add(new Pair(new LongWritable(), new Text("2\t1")));
    inputs.add(new Pair(new LongWritable(), new Text("3\t1 3 5")));
    inputs.add(new Pair(new LongWritable(), new Text("4\t1")));
    inputs.add(new Pair(new LongWritable(), new Text("5\t1 3")));
    mapReduceDriver.withAll(inputs);
    
    
    List<Pair<IntWritable,Text>> values = new ArrayList<Pair<IntWritable,Text>>();
    
    values.add(new Pair(new IntWritable(1), new Text("1 2 3 4 5")));
    values.add(new Pair(new IntWritable(2), new Text("1")));
    values.add(new Pair(new IntWritable(3), new Text("1")));
    values.add(new Pair(new IntWritable(4), new Text("1")));
    values.add(new Pair(new IntWritable(5), new Text("1")));

    
    mapReduceDriver.withAllOutput(values);

    mapReduceDriver.runTest();

  }
   
    
}
