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
package edu.stefano.forti.pad.connectedcomponents;

import edu.stefano.forti.pad.countnodes.CountNodes;
import edu.stefano.forti.pad.verifier.Verifier;
import edu.stefano.forti.pad.export.Export;
import edu.stefano.forti.pad.hashtomin.HashToMin;
import edu.stefano.forti.pad.hashtominsecondarysort.HashToMinSecondarySort;
import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

/**
 *
 * @author stefano
 */
public class ConnectedComponents {
    
    private final Path input, output;
    private final int reduceTasksNumber;
    private final boolean verifyResult;
    private final boolean secondarySort;
    private final FileSystem fileSystem;
    private final static int MAX_ITERATIONS = 30; 
   

    public ConnectedComponents(String input, String output, int reduceTasksNumber, boolean verifyResult, boolean secondarySort) throws IOException{
        this.input = new Path(input);
        this.output = new Path(output);
        this.reduceTasksNumber = reduceTasksNumber;
        this.verifyResult = verifyResult;
        this.fileSystem = FileSystem.get(new Configuration());
        this.secondarySort = secondarySort;
    }

    /**
     * @param args the command line arguments
     * @return 
     * @throws java.lang.Exception
     */
    public boolean run(String[] args) throws Exception {
        int iterate = 1;
        int iterations = 0;
        Path inputTmp, outputTmp = null;
        HtM hashToMin = null;
        CountNodes countNodes = null;
        
        if(verifyResult){
            countNodes = new CountNodes(input, reduceTasksNumber);
            countNodes.run(args);
            this.fileSystem.delete(new Path("tmp"), true);
        }
   
        while (iterate > 0 && iterations < MAX_ITERATIONS) {

            if (iterations == 0) {
                inputTmp = this.input;
            } else {
                inputTmp = this.output.suffix(Integer.toString(iterations));
            }

            outputTmp = output.suffix(Integer.toString(iterations + 1));
            
            
            if (secondarySort)
                hashToMin = new HashToMinSecondarySort(inputTmp, outputTmp, this.reduceTasksNumber);
            else 
                hashToMin = new HashToMin(inputTmp, outputTmp, this.reduceTasksNumber);
            
            iterate = hashToMin.run(null);

            if (iterations != 0) {
                this.fileSystem.delete(inputTmp, true);
            }

            iterations++;
        }

        Export export = new Export(outputTmp, output);
        export.run(null);
        this.fileSystem.delete(outputTmp, true);

        if (verifyResult){
            Verifier verifier = new Verifier(output);
            verifier.run(null);
            fileSystem.delete(new Path("tmp"), true);
            
            long duplicates = verifier.getDuplicates();
            long vertecesEnd = verifier.getVertecesEnd();
            long vertecesStart = -1, malformedLines = -1;
            if (countNodes!=null){
                vertecesStart = countNodes.getVertecesStart();
                malformedLines = countNodes.getMalformedLines();
            }
            
            boolean check = duplicates == 0 && vertecesStart == vertecesEnd;
            
            System.out.println("Start. Verteces number: "+ vertecesStart);
            System.out.println("Start. Malformed lines: " + malformedLines);
            System.out.println("End. Verteces number: "+ vertecesEnd);
            System.out.println("End. Duplicates: "+ duplicates);
            System.out.println("Verifier test passed: " + Boolean.toString(check).toUpperCase());
            
        }
        
        if (iterations > 1)
            System.out.println("Connected Components in " + iterations + " rounds.");
        else
            System.out.println("Connected Components in " + iterations + " round.");
        
        
        return true;
    }
    
    public static void main(String[] args) throws Exception {
        ConnectedComponents connected = new ConnectedComponents(args[0], args[1], Integer.parseInt(args[2]), true, false);
        connected.run(null);
    }
    
}
