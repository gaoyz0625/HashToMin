# HashToMin
A Hadoop MapReduce implementation of the HashToMin algorithm for finding connected components in a graph, starting from an input file either specifying the edges of the graph or the adjacency lists for each node.

The usage is fairly simple and it is listed below.
```java
ConnectedComponents connected = new ConnectedComponents(args[0], args[1], Integer.parseInt(args[2]), true);
connected.run(null);
```
