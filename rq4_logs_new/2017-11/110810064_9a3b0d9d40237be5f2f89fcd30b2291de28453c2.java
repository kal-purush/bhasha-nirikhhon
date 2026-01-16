package com.makebono.algorithms.computationalgeometry.convexhull;

import java.util.ArrayList;

import com.makebono.algorithms.computationalgeometry.convexhull.tools.HullContainer;
import com.makebono.datastructures.graph.BonoGraph;
import com.makebono.datastructures.graph.Edge;
import com.makebono.datastructures.graph.Vertex;
import com.makebono.datastructures.graph.graphInterface.Graph;

/** 
 * @ClassName: ConvexHullGenerator 
 * @Description: Abstract class for convex hull generator
 * @author makebono
 * @date 2017年11月17日 下午4:57:52 
 *  
 */
public abstract class ConvexHullGenerator<T> {
    private final Graph<T> graph;

    public ConvexHullGenerator() {
        this.graph = new BonoGraph<T>();
    }

    public Graph<T> getGraph() {
        return this.graph;
    }

    public void add(final int index, final T data, final double x, final double y) {
        final Vertex<T> newVertex = new Vertex<T>(index, data, x, y);
        this.graph.add(newVertex);
    }

    // Vertex with minimum y value / at the bottom of the graph. If several bottom lying vertices exist, pick the left
    // most one.
    protected Vertex<T> minimumY() {
        Vertex<T> target = this.getGraph().getVertices().get(0);

        for (final Vertex<T> temp : this.getGraph().getVertices()) {
            if (temp.getY() < target.getY()) {
                target = temp;
            } else if (temp.getY() == target.getY()) {
                if (temp.getX() <= target.getX()) {
                    target = temp;
                }
            }
        }

        return target;
    }

    protected Vertex<T> maximumY() {
        Vertex<T> target = this.getGraph().getVertices().get(0);

        for (final Vertex<T> temp : this.getGraph().getVertices()) {
            if (temp.getY() > target.getY()) {
                target = temp;
            } else if (temp.getY() == target.getY()) {
                if (temp.getX() <= target.getX()) {
                    target = temp;
                }
            }
        }

        return target;
    }

    // Return an array list of edge of the convex hull.
    public HullContainer<T> convexHull() {
        final ArrayList<Edge<T>> hull = new ArrayList<Edge<T>>();
        final ArrayList<Vertex<T>> vertices = this.candidates();
        hull.addAll(this.getGraph().getEdges());
        return new HullContainer<T>(vertices, hull);
    }

    protected abstract ArrayList<Vertex<T>> candidates();

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder();
        final HullContainer<T> chc = this.convexHull();

        sb.append("Convex hull of this graph consists Vertices:\n    ");
        for (final Vertex<T> temp : chc.getVertices()) {
            sb.append(temp);
        }
        sb.append("\nAnd edges:\n    ");
        for (final Edge<T> temp : chc.getEdges()) {
            sb.append(temp);
        }
        return sb.toString();

    }
}