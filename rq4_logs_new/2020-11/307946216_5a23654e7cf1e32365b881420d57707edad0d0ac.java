package hw.lesson7;

public class HomeworkGraphTest {
    public static void main(String[] args) {
        Graph graph = new Graph(10);

        graph.addVertex("Moscow");
        graph.addVertex("Tula");
        graph.addVertex("Ryazan");
        graph.addVertex("Kaluga");
        graph.addVertex("Lipetsk");
        graph.addVertex("Tambov");
        graph.addVertex("Orel");
        graph.addVertex("Saratov");
        graph.addVertex("Kursk");
        graph.addVertex("Voronezh");

        graph.addEdges("Moscow", "Tula");
        graph.addEdges("Moscow", "Ryazan");
        graph.addEdges("Moscow", "Kaluga");
        graph.addEdges("Tula", "Lipetsk");
        graph.addEdges("Lipetsk", "Voronezh");
        graph.addEdges("Ryazan", "Tambov");
        graph.addEdges("Tambov", "Saratov");
        graph.addEdges("Saratov", "Voronezh");
        graph.addEdges("Kaluga", "Orel");
        graph.addEdges("Orel", "Kursk");
        graph.addEdges("Kursk", "Voronezh");

//        graph.displayShortestWay(graph.searchBfsShortestWay("Lipetsk", "Orel"));
//        graph.displayShortestWay(graph.searchBfsShortestWay("Lipetsk", "Kaluga"));
        graph.displayShortestWay(graph.searchBfsShortestWay("Lipetsk", "Tambov"));
    }
}