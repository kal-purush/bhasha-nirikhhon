package ru.nsu.kgurin;

import java.util.Objects;

/**
 * Edge is a basic element of a graph that connects two vertices.
 */
public class Edge<T> {
    private final int weight;
    private final Vertex<T> vertexFrom;
    private final Vertex<T> vertexTo;

    public Edge(Vertex<T> vertexFrom, Vertex<T> vertexTo, int weight) {
        this.weight = weight;
        this.vertexFrom = vertexFrom;
        this.vertexTo = vertexTo;
    }

    /**
     * Get the vertex that the edge enter.
     *
     * @return vertex
     */
    public Vertex<T> getVertexTo() {
        return this.vertexTo;
    }

    /**
     * Get the vertex that the edge exit.
     *
     * @return vertex
     */
    public Vertex<T> getVertexFrom() {
        return this.vertexFrom;
    }

    /**
     * Get weight of edge.
     *
     * @return weight
     */
    public int getWeight() {
        return this.weight;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof Edge)) {
            return false;
        }
        Edge<?> edge = (Edge<?>) o;
        return Objects.equals(vertexFrom, edge.vertexFrom) && Objects.equals(vertexTo, edge.vertexTo)
                && Objects.equals(weight, edge.weight);
    }

    @Override
    public int hashCode() {
        return Objects.hash(vertexFrom, vertexTo, weight);
    }
}