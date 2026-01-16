import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;

class Boruvka
{
    class Edge
    {
        int v1;
        int v2;
        int weight;
    }

    private final static int INF = -1;

    private ArrayList<ArrayList<Integer>> mst;
    private int size_graph;
    private int[][] graph;

    public Boruvka(int size_graph, int[][] graph)
    {
        mst = new ArrayList<>();
        for (int i = 0; i < size_graph; i++)
        {
            mst.add(new ArrayList<>());
        }

        this.size_graph = size_graph;
        this.graph = graph;
    }

    private int getClosestVertex (int v)
    {
        int res = INF;
        int weight = INF;
        int i = 0;

        for (; i < size_graph; i++)
        {
            if (graph[v][i] != INF)
            {
                res = i;
                weight = graph[v][i];
                break;
            }
        }

        for (++i; i < size_graph; i++)
        {
            if (graph[v][i] != INF && graph[v][i] < weight)
            {
                res = i;
                weight = graph[v][i];
            }
        }

        return res;
    }

    private void dfs(int v, boolean[] used, ArrayList<ArrayList<Integer>> component, ArrayList<ArrayList<Integer>> fullMST)
    {
        used[v] = true;
        for (int i = 0; i < size_graph; i++)
        {
            if (fullMST.get(v).contains(i) && !used[i])
            {
                component.get(v).add(i);
                dfs(i, used, component, fullMST);
            }
        }
    }

    private boolean allUsed(boolean[] used)
    {
        for (int i = 0; i < used.length; i++)
        {
            if (!used[i])
            {
                return false;
            }
        }
        return true;
    }

    private boolean isInComponent(ArrayList<ArrayList<Integer>> component, int v)
    {
        for (int i = 0; i < size_graph; i++)
        {
            for (int j = 0; j < component.get(i).size(); j++)
            {
                if (v == component.get(i).get(j))
                {
                    return true;
                }
            }
            if (component.get(i).size() != 0)
            {
                if (v == i)
                {
                    return true;
                }
            }
        }

        return false;
    }

    private Edge getClosestToComponent(ArrayList<ArrayList<Integer>> component)
    {
        int weight = INF;
        int v = INF;
        boolean first = true;
        Edge e = new Edge();
        for (int i = 0; i < size_graph; i++)
        {
            for (int j = 0; j < component.get(i).size(); j++)
            {

                int k = 0;
                if (first)
                {
                    for (; k < size_graph; k++)
                    {
                        if (!isInComponent(component, k) && graph[component.get(i).get(j)][k] != INF)
                        {
                            weight = graph[component.get(i).get(j)][k];
                            v = k;
                            e.v1 = component.get(i).get(j);
                            e.v2 = v;
                            e.weight = weight;
                            first = false;
                            break;
                        }
                    }
                }
                else
                {
                    k--;
                }

                for (++k; k < size_graph; k++)
                {
                    if (!isInComponent(component, k) && graph[component.get(i).get(j)][k] != INF && graph[component.get(i).get(j)][k] < weight)
                    {
                        weight = graph[component.get(i).get(j)][k];
                        v = k;
                        e.v1 = component.get(i).get(j);
                        e.v2 = v;
                        e.weight = weight;
                    }
                }
            }
            if (component.get(i).size() != 0)
            {
                int k = 0;
                for (; k < size_graph; k++)
                {
                    if (!isInComponent(component, k) && graph[i][k] != INF)
                    {
                        weight = graph[i][k];
                        v = k;
                        e.v1 = i;
                        e.v2 = v;
                        e.weight = weight;
                    }
                    break;
                }
                for (++k; k < size_graph; k++)
                {
                    if (!isInComponent(component, k) && graph[i][k] != INF && graph[i][k] < weight)
                    {
                        weight = graph[i][k];
                        v = k;
                        e.v1 = i;
                        e.v2 = v;
                        e.weight = weight;
                    }
                }
            }
        }

        return (v != INF) ? e : null;
    }

    private int getMSTEdgesCount()
    {
        HashSet<Edge> edges = new HashSet<>();
        boolean exists;
        for (int i = 0; i < size_graph; i++)
        {
            for (int j = 0; j < mst.get(i).size(); j++)
            {
                if (i != mst.get(i).get(j))
                {
                    exists = false;
                    for (Edge e : edges)
                    {
                        if ((i == e.v1 && mst.get(i).get(j) == e.v2) || (i == e.v2 && mst.get(i).get(j) == e.v1))
                        {
                            exists = true;
                            break;
                        }
                    }
                    if (!exists)
                    {
                        Edge e = new Edge();
                        e.v1 = i;
                        e.v2 = mst.get(i).get(j);
                        e.weight = graph[i][mst.get(i).get(j)];
                        edges.add(e);
                    }
                }
            }
        }
        return edges.size();
    }

    private boolean edgeExists(Edge e)
    {
        for (int i = 0; i < mst.size(); i++)
        {
            for (int j = 0; j < mst.get(i).size(); j++)
            {
                if ((e.v1 == i && e.v2 == j) || (e.v1 == j && e.v2 == i))
                {
                    return true;
                }
            }
        }

        return false;
    }

    private ArrayList<ArrayList<ArrayList<Integer>>> getComponentsList()
    {
        ArrayList<ArrayList<ArrayList<Integer>>> components = new ArrayList<>();
        boolean[] used = new boolean[size_graph];
        Arrays.fill(used, false);
        ArrayList<ArrayList<Integer>> fullMST = new ArrayList<>();
        HashSet<Edge> edges = new HashSet<>();
        for (int i = 0; i < mst.size(); i++) {
            for (int j = 0; j < mst.get(i).size(); j++)
            {
                boolean exists = false;
                for (Edge e : edges)
                {
                    if ((e.v1 == i && e.v2 == mst.get(i).get(j)) || (e.v1 == mst.get(i).get(j) && e.v2 == i))
                    {
                        exists = true;
                        break;
                    }
                }
                if (!exists)
                {
                    Edge e2 = new Edge();
                    e2.v1 = i;
                    e2.v2 = mst.get(i).get(j);
                    e2.weight = graph[i][mst.get(i).get(j)];
                    edges.add(e2);
                }
            }
        }
        for (int i = 0; i < size_graph; i++)
        {
            fullMST.add(new ArrayList<>());
        }
        for (Edge e : edges)
        {
            fullMST.get(e.v1).add(e.v2);
            fullMST.get(e.v2).add(e.v1);
        }
        while (!allUsed(used))
        {
            ArrayList<ArrayList<Integer>> component = new ArrayList<>();
            for (int i = 0; i < size_graph; i++)
            {
                component.add(new ArrayList<>());
            }
            for (int i = 0; i < size_graph; i++)
            {
                if (!used[i])
                {
                    dfs(i, used, component, fullMST);
                    components.add(component);
                    component = new ArrayList<>();
                    for (int j = 0; j < size_graph; j++)
                    {
                        component.add(new ArrayList<>());
                    }
                }
            }
        }

        return components;
    }

    public void Boruvka ()
    {
        for (int i = 0; i < size_graph; i++)
        {
            int closestVert = getClosestVertex(i);
            mst.get(i).add(closestVert);
        }

        while (getMSTEdgesCount() < (size_graph - 1))
        {
            ArrayList<ArrayList<ArrayList<Integer>>> components = getComponentsList();
            for (int i = 0; i < components.size(); i++)
            {
                Edge e = getClosestToComponent(components.get(i));
                if (e != null && !edgeExists(e)) {
                    mst.get(e.v1).add(e.v2);
                }
            }
        }

        HashSet<Edge> edges = new HashSet<>();
        for (int i = 0; i < mst.size(); i++)
        {
            for (int j = 0; j < mst.get(i).size(); j++)
            {
                boolean exists = false;
                for (Edge e : edges)
                {
                    if ((e.v1 == i && e.v2 == mst.get(i).get(j)) || (e.v1 == mst.get(i).get(j) && e.v2 == i))
                    {
                        exists = true;
                        break;
                    }
                }
                if (!exists)
                {
                    Edge e = new Edge();
                    e.v1 = i;
                    e.v2 = mst.get(i).get(j);
                    edges.add(e);
                }
            }
        }
        mst.clear();
        for (int i = 0; i < size_graph; i++)
        {
            mst.add(new ArrayList<>());
        }
        for (Edge e : edges)
        {
            mst.get(e.v1).add(e.v2);
            mst.get(e.v2).add(e.v1);
        }
    }

    public void printTree()
    {
        System.out.println(mst);
    }
}