package core;

public class Edge implements GraphComponent {
    public Vertex vs, ve;
    public int weight = 0;

    public Edge(){}

    public Edge(Vertex vs, Vertex ve, int weight) {
        this.vs = vs;
        this.ve = ve;
        this.weight = weight;
    }
}