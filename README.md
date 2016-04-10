# HashToMin
A Hadoop MapReduce implementation of the [HashToMin algorithm](http://citeseerx.ist.psu.edu/viewdoc/download?doi=10.1.1.384.4810&rep=rep1&type=pdf) for finding connected components in a graph, starting from an input file either specifying the edges of the graph or the adjacency lists for each node. Each line of the input ﬁle represents either an already formed cluster within the graph G or an edge of the graph. Vertex identiﬁers must be separated by a space or a tab. The output ﬁle will contain one connected component per line, with the ﬁrst node representing its label, followed by a tab and all the cluster's nodes divided by spaces. Sample input ﬁles can be found in the folder inputfiles.

The usage is fairly simple and it is listed below. Instantiate the class
```java
public ConnectedComponents (String input,
                            String output, 
                            int reduceTasksNumber,
                            boolean verifyResult,
                            boolean secondarySort) 
```
where:
- `input` and `output` specify the input and output ﬁle paths,
- `reduceTasksNumber` speciﬁes the number of reducers available and to be exploited in all jobs but the Export procedure (that must output a single ﬁle),
- `verifyResult` that is used to execute the CountNodes and the Verifier job if it is set to `true`,
- `secondarySort` to decide which version of the algorithm to use, HashToMinSecondarySort runs when this attribute is `true`.

Then call the method `run()` over the new object. 

Alternatively, the jar can be run on some input issuing the command
```bash
hadoop jar ./target/HashToMin-1.0.jar <input> <output> <numberOfReducers>
```
from the project folder.
