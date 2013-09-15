/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package ru.nullpointer.nkbrelation.components;

import java.io.IOException;
import org.apache.giraph.edge.Edge;
import org.apache.giraph.graph.BasicComputation;
import org.apache.giraph.graph.Vertex;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Implementation of the HCC algorithm that identifies connected components and
 * assigns each vertex its "component identifier" (the smallest vertex id in the
 * component)
 *
 * The idea behind the algorithm is very simple: propagate the smallest vertex
 * id along the edges to all vertices of a connected component. The number of
 * supersteps necessary is equal to the length of the maximum diameter of all
 * components + 1
 *
 * The original Hadoop-based variant of this algorithm was proposed by Kang,
 * Charalampos, Tsourakakis and Faloutsos in "PEGASUS: Mining Peta-Scale
 * Graphs", 2010
 *
 * http://www.cs.cmu.edu/~ukang/papers/PegasusKAIS.pdf
 */
public class ConnectedComponentsComputation extends BasicComputation<Text, Text, NullWritable, Text> {

    private Logger logger = LoggerFactory.getLogger(ConnectedComponentsComputation.class);
    //

    /**
     * Propagates the smallest vertex id to all neighbors. Will always choose to
     * halt and only reactivate if a smaller id has been sent to it.
     *
     * @param vertex Vertex
     * @param messages Iterator of messages from the previous superstep.
     * @throws IOException
     */
    @Override
    public void compute(Vertex<Text, Text, NullWritable> vertex, Iterable<Text> messages) throws IOException {
        if (getSuperstep() == 0) {
            doFirstStep(vertex);
        } else {
            doStep(vertex, messages);
        }
        vertex.voteToHalt();
    }

    private void doFirstStep(Vertex<Text, Text, NullWritable> vertex) {
        // First superstep is special, because we can simply look at the neighbors
        // On first step value is not set, so using id
        Text currentComponent = vertex.getId();

        for (Edge<Text, NullWritable> edge : vertex.getEdges()) {
            Text neighbor = edge.getTargetVertexId();
            if (neighbor.compareTo(currentComponent) < 0) {
                currentComponent = new Text(neighbor); // do clone since neighbor is mutable
            }
        }
        vertex.setValue(currentComponent);
        for (Edge<Text, NullWritable> edge : vertex.getEdges()) {
            Text neighbor = edge.getTargetVertexId();
            if (neighbor.compareTo(currentComponent) > 0) {
                sendMessage(neighbor, currentComponent);
            }
        }
    }

    private void doStep(Vertex<Text, Text, NullWritable> vertex, Iterable<Text> messages) {
        Text currentComponent = vertex.getValue();
        boolean changed = false;
        // did we get a smaller id ?
        for (Text message : messages) {
            Text candidateComponent = message;
            if (candidateComponent.compareTo(currentComponent) < 0) {
                currentComponent = new Text(candidateComponent); // do clone since candidateComponent is mutable
                changed = true;
            }
        }
        // propagate new component id to the neighbors
        if (changed) {
            vertex.setValue(currentComponent);
            sendMessageToAllEdges(vertex, currentComponent);
        }
    }

    private String getEdges(Vertex<Text, Text, NullWritable> vertex) {
        StringBuilder sb = new StringBuilder();
        for (Edge<Text, NullWritable> edge : vertex.getEdges()) {
            sb.append(sb.length() > 0 ? ", " : "");
            sb.append(edge.getTargetVertexId());
        }
        return sb.toString();
    }
}
