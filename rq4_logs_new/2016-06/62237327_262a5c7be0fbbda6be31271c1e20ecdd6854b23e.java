import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashSet;

/**
 * Created by Vitalik on 29.06.2016.
 */

class Kruskal
{
    class Edge
    {
        int v1;
        int v2;
        int weight;
    }

    int size_graph;
    int graph[][];
    private final static int INF = -1;

    Kruskal(Graph g)
    {
        size_graph = g.size_graph;
        graph = new int [size_graph][size_graph];
        for (int i = 0; i < size_graph; i++)
            for(int j = 0; j < size_graph; j++)
                graph[i][j] = g.graph[i][j];
    }

    class Comp implements Comparator<Edge>
    {
        @Override
        public int compare(Edge o1, Edge o2)
        {
            if (o1.weight < o2.weight)
                return -1;
            else if (o1.weight == o2.weight)
                return 0;
            else
                return 1;
        }
    }

    ArrayList<Edge> Create_List()
    {
        ArrayList<Edge> list = new ArrayList<Edge>();
        for (int i = 0; i < size_graph; i++)
            for (int j = i + 1; j < size_graph; j++)
            {
                if (graph[i][j] != -1)
                {
                    Edge e = new Edge();
                    e.v1 = i;
                    e.v2 = j;
                    e.weight = graph[i][j];
                    list.add(e);
                }
            }
        list.sort(new Comp());
        return list;
    }

    public void Kruskal()
    {
        HashSet<Integer> subgraph = new HashSet <Integer>();
        for (int i = 0; i < size_graph; i++)
        {
            subgraph.add(i);
        }
        ArrayList<Edge> edges_list = Create_List();
        for (int i = 0; i < size_graph; i++)
            for (int j = 0; j < size_graph; j++)
            {

            }
    }
}