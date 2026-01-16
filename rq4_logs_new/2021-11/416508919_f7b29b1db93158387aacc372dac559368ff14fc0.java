package graph;

public class Edges <T> {
    private graph.Node node;
    int Weight;

    public Edges(graph.Node node, int Weight) {
        this.node = node;
        this.Weight = Weight;
    }

    public graph.Node getNode() {
        return node;
    }

    public void setVertex(graph.Node node) {
        this.node = node;
    }

    @Override
    public String toString() {
        return "Edges{" +
                "vertex=" + node +
                ", weight=" + Weight +
                '}';
    }
}