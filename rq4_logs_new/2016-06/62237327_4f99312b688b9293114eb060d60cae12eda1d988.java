import java.util.ArrayList;
import java.util.HashSet;

/**
 * Created by Vitalik on 29.06.2016.
 */

public class Prim
{
    int size_graph;
    int graph[][];
    private final static int INF = -1;
    ArrayList <ArrayList<Integer>> min_spaning_tree = new ArrayList <ArrayList <Integer>>();
    ArrayList <Integer> min_spaning_tree_weight = new ArrayList <Integer>();

    Prim(Graph g)
    {
        size_graph = g.size_graph;
        graph = new int [size_graph][size_graph];
        for (int i = 0; i < size_graph; i++)
            for(int j = 0; j < size_graph; j++)
                graph[i][j] = g.graph[i][j];
    }

    private int min(HashSet<Integer> Q)
    {
        int min = INF;
        int res = INF;
        for (int v : Q)
        {
            min = min_spaning_tree_weight.get(v);
            res = v;
            break;
        }
        for (int v : Q)
        {
            if (min_spaning_tree_weight.get(v) != INF && min_spaning_tree_weight.get(v) < min)
            {
                min = min_spaning_tree_weight.get(v);
                res = v;
            }
        }

        return res;
    }

    public void Prim()
    {
        for (int i = 0; i < size_graph; i++)
        {
            ArrayList <Integer> a = new ArrayList <Integer>();
            min_spaning_tree.add(a);
        }
        ArrayList <Integer> parent = new ArrayList <Integer>();
        HashSet <Integer> Q = new HashSet <Integer>();
        for (int i = 0; i < size_graph; i++)
        {
            min_spaning_tree_weight.add(INF);
            parent.add(null);
            Q.add(i);
        }
        min_spaning_tree_weight.set(0, 0);

        while (!Q.isEmpty())
        {
            int v;
            v = min(Q);
            Q.remove(v);
            if (Q.isEmpty()) {
                break;
            }
            for (int i = 0; i < size_graph; i++)
            {
                if (graph[v][i] != INF && Q.contains(i) && (graph[v][i] < min_spaning_tree_weight.get(i) || min_spaning_tree_weight.get(i) == INF))
                {
                    min_spaning_tree_weight.set(i, graph[v][i]);
                    parent.set(i, v);
                }
            }
            v = min(Q);
            min_spaning_tree.get(parent.get(v)).add(v);
        }
    }

    public void Print_Tree()
    {
        System.out.println(min_spaning_tree);
    }
}
