import java.util.*;

public class DirectedGraph {

    private HashMap<Vertex, List<Edge>> vertexMap = new HashMap<Vertex, List<Edge>>();

   public void changeVertexName(Vertex vertex, String name){
       vertex.setVertexName(name);
   }

   public void changeEdgeWeight(String v1, String v2, int weight, int newWeight){
       Edge edge = new Edge(getKey(v1), getKey(v2), weight);
       edge.setWeight(newWeight);
   }

    public void addVertex(String vertexName) {
        if (!vertexExist(vertexName)) {
            vertexMap.put(new Vertex(vertexName), new ArrayList<Edge>());
        }
    }

    public boolean vertexExist(String vertexName) {
        if (getKey(vertexName) == null) return false;
        return vertexMap.containsKey(getKey(vertexName));
    }

    public boolean edgeExist(String firstVertex, String secondVertex) {
        // Edge edge = new Edge(firstVertex, secondVertex);
        if (!vertexExist(firstVertex) || !vertexExist(secondVertex)) return false;
        List<Edge> edges = vertexMap.get(getKey(firstVertex));
        for (Edge edge : edges) {
            if (edge.getFirstNode().getVertexName().equals(firstVertex) &&
                    edge.getSecondNode().getVertexName().equals(secondVertex)) return true;
        }
        return false;
    }

    public void addEdge(String firstVertex, String secondVertex, Integer weight) {

        //if (!edgeExist(vertex1, vertex2)) {
        if (!vertexExist(firstVertex)) addVertex(firstVertex);
        if (!vertexExist(secondVertex)) addVertex(secondVertex);
        Vertex vertex1 = getKey(firstVertex);
        Vertex vertex2 = getKey(secondVertex);
        List<Edge> edges = getValue(firstVertex);
        edges.add(new Edge(vertex1, vertex2, weight));
        //Collections.sort(edges);
        //}
    }

    public Map<Vertex, List<Edge>> getGraph() {
        return vertexMap;
    }

    private List<List<Edge>> getValues() {
        List<List<Edge>> values = new ArrayList<List<Edge>>();
        for (Vertex key : getKeys()) {
            values.add(vertexMap.get(key));
        }
        return values;
    }

    public Vertex getKey(String vertexName) {

        for (Vertex key : getKeys()) {
            if (key.getVertexName().equals(vertexName)) return key;
        }
        return null;
    }

    public List<Edge> getValue(String name) {
        List<Edge> value = new ArrayList<Edge>();
        for (Vertex key : getKeys()) {
            if (key.getVertexName().equals(name)) value = vertexMap.get(key);
        }
        return value;
    }

    private List<Vertex> getKeys() {
        return new ArrayList<Vertex>(vertexMap.keySet());
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        for (Vertex key : getKeys()) {
            sb.append(key.getVertexName() + ":\n");
            for (Edge edge : vertexMap.get(key)) {
                //for (Edge edge: value) {
                sb.append(" -> " + edge.getSecondNode().getVertexName() + " (W: " + edge.getWeight() + ");\n");
                //}

            }
        }
        return sb.toString();
    }


    @Override
    public int hashCode(){
        return 32 + this.getGraph().hashCode() * 8;
    }

    @Override
    public boolean equals(Object o){
        if (o == null) return false;
        if (!o.equals(this)) return false;
        if (o.getClass() != this.getClass()) return false;
        DirectedGraph dg = (DirectedGraph) o;
        return dg.getGraph().equals(this.getGraph());
    }

}