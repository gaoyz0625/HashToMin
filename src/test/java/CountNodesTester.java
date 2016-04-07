
import edu.stefano.forti.pad.connectedcomponents.JobCounters;
import edu.stefano.forti.pad.countnodes.CountNodesMapper;
import edu.stefano.forti.pad.countnodes.CountNodesReducer;
import edu.stefano.forti.pad.hashtomin.ClusterWritable;
import edu.stefano.forti.pad.hashtomin.HashToMinMapper;
import edu.stefano.forti.pad.hashtomin.HashToMinReducer;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.apache.hadoop.mrunit.types.Pair;
import static org.junit.Assert.assertEquals;
import org.junit.Before;
import org.junit.Test;

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
/**
 *
 * @author stefano
 */
public class CountNodesTester {

    static MapDriver<LongWritable, Text, IntWritable, NullWritable> mapDriver;
    static ReduceDriver<IntWritable, NullWritable, NullWritable, NullWritable> reduceDriver;
    static MapReduceDriver<LongWritable, Text, IntWritable, NullWritable, NullWritable, NullWritable> mapReduceDriver;

    @Before
    public void setUp() {
        CountNodesMapper mapper = new CountNodesMapper();
        CountNodesReducer reducer = new CountNodesReducer();
        mapDriver = MapDriver.newMapDriver(mapper);
        reduceDriver = ReduceDriver.newReduceDriver(reducer);
        mapReduceDriver = MapReduceDriver.newMapReduceDriver(mapper, reducer);
    }

    @Test
    public void processesValidRecordMapper() throws IOException {
        NullWritable nw = NullWritable.get();

        mapDriver.withInput(new LongWritable(1), new Text("1\t1 2 4"))
                .withOutput(new IntWritable(1), nw)
                .withOutput(new IntWritable(1), nw)
                .withOutput(new IntWritable(2), nw)
                .withOutput(new IntWritable(4), nw);
        mapDriver.runTest();
    }

    @Test
    public void ignoreMalformedInputMapper() throws IOException {
        mapDriver.withInput(new LongWritable(1), new Text("#\t1 2 4"))
                .withInput(new LongWritable(1), new Text("malformed line"))
                .runTest();

        assertEquals("Expected 2 counter increment", 2, mapDriver.getCounters()
                .findCounter(JobCounters.MALFORMED_LINES).getValue());

    }

    @Test
    public void processesValidRecordReducer() throws IOException {
        NullWritable nw = NullWritable.get();

        List<NullWritable> list = new ArrayList<NullWritable>();
        list.add(nw);
        list.add(nw);

        reduceDriver.withInput(new IntWritable(1), list)
                .withInput(new IntWritable(3), list);
        reduceDriver.runTest();

        assertEquals("Expected 2 counter increment", 2, reduceDriver.getCounters()
                .findCounter(JobCounters.VERTECES_START).getValue());
    }

    @Test
    public void processesValidRecordMapReduce() throws IOException {
        List<Pair<LongWritable, Text>> inputs = new ArrayList<Pair<LongWritable, Text>>();
        inputs.add(new Pair(new LongWritable(), new Text("1\t2 3 4 5")));
        inputs.add(new Pair(new LongWritable(), new Text("2\t1")));
        inputs.add(new Pair(new LongWritable(), new Text("3\t1 3 5")));
        inputs.add(new Pair(new LongWritable(), new Text("4\t1")));
        inputs.add(new Pair(new LongWritable(), new Text("5\t1 3")));
        inputs.add(new Pair(new LongWritable(), new Text("##")));
        mapReduceDriver.withAll(inputs);

        mapReduceDriver.runTest();

        assertEquals("Expected 5 counter increment", 5, mapReduceDriver.getCounters()
                .findCounter(JobCounters.VERTECES_START).getValue());

        assertEquals("Expected 1 counter increment", 1, mapReduceDriver.getCounters()
                .findCounter(JobCounters.MALFORMED_LINES).getValue());

    }
}
