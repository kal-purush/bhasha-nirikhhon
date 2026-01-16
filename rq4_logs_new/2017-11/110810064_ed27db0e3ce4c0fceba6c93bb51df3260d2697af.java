package com.makebono.algorithms.networkflow.maximumflow;

import java.util.ArrayList;

import com.makebono.datastructures.graph.BonoGraph;
import com.makebono.datastructures.graph.Edge;
import com.makebono.datastructures.graph.Vertex;
import com.makebono.datastructures.graph.graphInterface.Graph;

/** 
 * @ClassName: FordFulkersonMethod 
 * @Description: Ford-Fulkerson method for finding maximum flow in a flow network. With Edmonds-Karp algorithm as I used 
 * BFS for path finding.
 * @author makebono
 * @date 2017年11月24日 上午10:25:24 
 *  
 */
public class FordFulkersonMethod {
    private final Graph<String> residualGraph;

    public FordFulkersonMethod() {
        this.residualGraph = new BonoGraph<String>();
    }

    public ArrayList<Edge<String>> maximumFlow() {
        final ArrayList<Edge<String>> result = new ArrayList<Edge<String>>();

        ArrayList<Vertex<String>> path = this.getResidualGraph().bfsPath().getResult();
        /*
        for (final Vertex<String> cursor : path) {
            System.out.println(cursor);
        }
        */

        while (path.size() != 0) {
            final ArrayList<Edge<String>> augmentingPath = new ArrayList<Edge<String>>();
            final boolean[] isInverted = new boolean[path.size() - 1];

            for (int i = 0; i < path.size() - 1; i++) {
                final Vertex<String> v1 = path.get(i);
                final Vertex<String> v2 = path.get(i + 1);
                final Edge<String> temp = v1.getEdge(v2);

                // System.out.println(temp);

                if (temp != null) {
                    isInverted[i] = temp.setResidualCapacity(v1, v2);
                } else {
                    System.out.println("Unexpected error occurs.");
                    return null;
                }

                augmentingPath.add(temp);
            }

            int minResidualCapacity = augmentingPath.get(0).getResidualCapacity();

            for (final Edge<String> min : augmentingPath) {
                if (minResidualCapacity > min.getResidualCapacity()) {
                    minResidualCapacity = min.getResidualCapacity();
                }
            }

            for (int i = 0; i < augmentingPath.size(); i++) {

                if (isInverted[i]) {
                    augmentingPath.get(i).setFlow(augmentingPath.get(i).getFlow() - minResidualCapacity);

                } else {
                    augmentingPath.get(i).setFlow(augmentingPath.get(i).getFlow() + minResidualCapacity);
                }

                // System.out.println(augmentingPath.get(i).getResidualCapacity());
            }
            path = this.getResidualGraph().bfsPath().getResult();
        }

        result.addAll(this.getResidualGraph().getEdges());
        this.printMaximumFlow(result);
        return result;
    }

    public void add(final Vertex<String> v1, final Vertex<String> v2, final int capacity) {
        this.residualGraph.add(v1, v2, capacity, 0);
    }

    public Graph<String> getResidualGraph() {
        return this.residualGraph;
    }

    public Vertex<String> getSource() {
        return this.getResidualGraph().getSource();
    }

    public Vertex<String> getSink() {
        return this.getResidualGraph().getSink();
    }

    public void setSource(final Vertex<String> source) {
        this.getResidualGraph().setSource(source);
    }

    public void setSink(final Vertex<String> sink) {
        this.getResidualGraph().setSink(sink);
    }

    private void printMaximumFlow(final ArrayList<Edge<String>> result) {
        System.out.printf("Below shows the maximum flow:\n");
        for (final Edge<String> cursor : result) {
            System.out.printf("Edge(%d, %d, %d) ", cursor.getV1().getIndex(), cursor.getV2().getIndex(),
                    cursor.getFlow());
        }
    }
}