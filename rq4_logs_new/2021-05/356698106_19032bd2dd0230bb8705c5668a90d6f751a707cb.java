package javaChallenges.graph;

import java.util.ArrayList;

public class Graph {
  ArrayList<Object> vertexes;
  ArrayList<ArrayList<Object>> adjacencyMatrix;

  public Graph(){
    this.vertexes = new ArrayList<>();
    this.adjacencyMatrix = new ArrayList<>();
  }

  public Object AddVertex(Object value){
    this.vertexes.add(value);

    for(int i=0;i<adjacencyMatrix.size();i++)
    {
      adjacencyMatrix.get(i).add(0);
    }
    this.adjacencyMatrix.add(new ArrayList<>());

    for(int i=0 ;i<this.adjacencyMatrix.size();i++){
      this.adjacencyMatrix.get(adjacencyMatrix.size()-1).add(0);
    }
    return value;
  }

  public void AddEdge(Object value1, Object value2, int weight){
    if(value1 == null || value2 == null)return;
    int idx1 = this.vertexes.indexOf(value1);
    int idx2 = this.vertexes.indexOf(value2);
    this.adjacencyMatrix.get(idx1).set(idx2,weight);
  }

  public Object GetNodes(){
    return this.vertexes;
  }

  public int Size(){
    return this.vertexes.size();
  }

  public ArrayList<Object> GetNeighbors(Object vertex){
    int index = this.vertexes.indexOf(vertex);
    ArrayList<Object> neighbors = new ArrayList<>();
    ArrayList<Object> targetMatrix = adjacencyMatrix.get(index);
    for(Object neighbor: this.vertexes){
      int neighborIndex = vertexes.indexOf(neighbor);
      int neighborIdxInCurrArray = (int)targetMatrix.get(neighborIndex);
      if(neighbor != vertex && neighborIdxInCurrArray != 0){
        neighbors.add(neighbor + " -> " + targetMatrix.get(neighborIndex));
      }
    }
    return neighbors;
  }
  @Override
  public String toString() {
    StringBuilder aMatrix = new StringBuilder();
    for(Object arr: adjacencyMatrix){
      if(adjacencyMatrix.indexOf(arr) != adjacencyMatrix.size()-1){
        aMatrix.append(vertexes.get(adjacencyMatrix.indexOf(arr))).append(" = ").append(arr).append("\n");
      }else{
        aMatrix.append(vertexes.get(adjacencyMatrix.indexOf(arr))).append(" = ").append(arr);
      }
    }
    return "Graph{" +
      "\nvertexes=" + vertexes +
      ", \nadjacencyMatrix=\n" + aMatrix +
      "\n}";
  }

  public ArrayList<Object> getVertexes() {
    return vertexes;
  }

  public ArrayList<ArrayList<Object>> getAdjacencyMatrix() {
    return adjacencyMatrix;
  }
}