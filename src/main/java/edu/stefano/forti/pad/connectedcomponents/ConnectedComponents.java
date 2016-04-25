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
import java.util.logging.Level;
import java.util.logging.Logger;
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
   
    /**
     * Build an instance of the class managing the HashToMin Job.
     * @param input the input file
     * @param output the desired output file
     * @param reduceTasksNumber the number of reducers to be used
     * @param verifyResult true for veryfying the output
     * @param secondarySort true for using the secondary sort capability
     * @throws IOException 
     */
    public ConnectedComponents(String input, String output, int reduceTasksNumber, boolean verifyResult, boolean secondarySort) throws HtMException {
        this.input = new Path(input);
        this.output = new Path(output);
        this.reduceTasksNumber = reduceTasksNumber;
        this.verifyResult = verifyResult;
        try {
            this.fileSystem = FileSystem.get(new Configuration());
        } catch (IOException ex) {
           throw new HtMException(ex.getMessage());
        }
        this.secondarySort = secondarySort;
    }

    /**
     * @param args the command line arguments
     * @return
     * @throws edu.stefano.forti.pad.connectedcomponents.HtMException
     */
    public boolean run(String[] args) throws HtMException {
        int iterate = 1; //start HtM when > 0
        int iterations = 0; //counts the HtM rounds
        Path inputTmp, outputTmp = null;
        HtM hashToMin = null;
        CountNodes countNodes = null;
        try{
        if(verifyResult){
            countNodes = new CountNodes(input, reduceTasksNumber);
            countNodes.run(args);
            this.fileSystem.delete(new Path("tmp"), true);
        }
   
        while (iterate > 0 && iterations < MAX_ITERATIONS) {
            
            if (iterations == 0) { //input is the actual input file
                inputTmp = this.input;
            } else { //input is the previous output file
                inputTmp = this.output.suffix(Integer.toString(iterations));
            }

            outputTmp = output.suffix(Integer.toString(iterations + 1));
            
            //select the correct HtM version
            if (secondarySort)
                hashToMin = new HashToMinSecondarySort(inputTmp, outputTmp, reduceTasksNumber);
            else 
                hashToMin = new HashToMin(inputTmp, outputTmp, reduceTasksNumber);
            //wait for the job to complete
            iterate = hashToMin.run(null);
            //delete intermediate output
            if (iterations != 0) {
                this.fileSystem.delete(inputTmp, true);
            }

            iterations++;
        }
        //starts the Export procedure and awaits termination
        Export export = new Export(outputTmp, output, reduceTasksNumber);
        export.run(null);
        this.fileSystem.delete(outputTmp, true);
        //verify the output correctness if required
        if (verifyResult){
            Verifier verifier = new Verifier(output, reduceTasksNumber);
            verifier.run(null);
            fileSystem.delete(new Path("tmp"), true);
            
            long duplicates = verifier.getDuplicates();
            long vertecesEnd = verifier.getVertecesEnd();
            long vertecesStart = -1, malformedLines = -1;
            if (countNodes!=null){
                vertecesStart = countNodes.getVertecesStart();
                malformedLines = countNodes.getMalformedLines();
            }
            //the correctness check: no duplicates, all verteces in the output
            boolean check = duplicates == 0 && vertecesStart == vertecesEnd;
            //log info
            System.out.println("Start. Verteces number: "+ vertecesStart);
            System.out.println("Start. Malformed lines: " + malformedLines);
            System.out.println("End. Verteces number: "+ vertecesEnd);
            System.out.println("End. Duplicates: "+ duplicates);
            System.out.println("Verifier test passed: " + Boolean.toString(check).toUpperCase());
            
        }
        //log number of rounds
        if (iterations > 1)
            System.out.println("Connected Components in " + iterations + " rounds.");
        else
            System.out.println("Connected Components in " + iterations + " round.");

        return true;
        }
        catch (IOException  ex){
            throw new HtMException(ex.getMessage());
        } catch (InterruptedException ex) {
            throw new HtMException(ex.getMessage());
        } catch (ClassNotFoundException ex) {
            throw new HtMException(ex.getMessage());
        }
        
    }
    
    public static void main(String[] args) {
        try {
            ConnectedComponents connected = new ConnectedComponents(args[0], args[1], Integer.parseInt(args[2]), true, true);
            connected.run(null);
        } catch (HtMException ex) {
            Logger.getLogger(ConnectedComponents.class.getName()).log(Level.SEVERE, null, ex);
        }
    }
    
}
