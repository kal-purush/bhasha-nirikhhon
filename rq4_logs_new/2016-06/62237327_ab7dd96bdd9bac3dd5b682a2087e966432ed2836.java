/**
 * Created by Vitalik on 24.06.2016.
 */

import com.sun.javafx.geom.Edge;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.util.*;

public class Graph
{
    int graph[][];
    int size_graph;

    ArrayList<Integer> initU(int size)
    {
        ArrayList<Integer> U = new ArrayList<>();
        for(int i = 0; i < size; i++)
            U.add(i);
        return U;
    }

    Graph(String s)
    {
        try (Scanner sc = new Scanner(new FileReader(s)))
        {
            size_graph = sc.nextInt();
            initU(size_graph);
            graph = new int [size_graph][size_graph];
            for (int i = 0; i < size_graph; i++)
                for(int j = 0; j < size_graph; j++)
                    graph[i][j] = sc.nextInt();
        }
        catch (FileNotFoundException e)
        {
            e.printStackTrace();
        }
    }

    void Print_Matrix()
    {
        for(int i = 0; i < size_graph; i++)
        {
            for(int j = 0; j < size_graph; j++)
            {
                System.out.print(graph[i][j]+ " ");
            }
            System.out.println();
        }
    }

    public static void main(String args[])
    {
        Graph g = new Graph("src\\input.txt");
        g.Print_Matrix();
        Prim p = new Prim(g);
        p.Prim();
        p.Print_Tree();
    }
}
