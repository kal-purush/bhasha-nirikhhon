package data_structure;

import java.util.ArrayList;
import java.util.LinkedList;
/**
 * @description 邻接矩阵模型类
 */
public class GraphTraverse {
    private ArrayList<Object> vertexList;//存储点的链表
    private int[][] edges;//邻接矩阵，用来存储边
    private int numOfEdges;//边的数目
    private boolean[] isVisited;

    public GraphTraverse(int n) {
        //初始化矩阵，一维数组，和边的数目
        edges = new int[n][n];
        vertexList = new ArrayList<Object>(n);
        numOfEdges = 0;
    }
    
    public void initIsVisited(){
        isVisited = new boolean[vertexList.size()];    	
    }

    //得到结点的个数
    public int getNumOfVertex() {
        return vertexList.size();
    }

    //得到边的数目
    public int getNumOfEdges() {
        return numOfEdges;
    }

    //返回结点i的数据
    public Object getValueByIndex(int i) {
        return vertexList.get(i);
    }

    //返回v1,v2的权值
    public int getWeight(int v1,int v2) {
        return edges[v1][v2];
    }

    //插入结点
    public void insertVertex(Object vertex) {
        vertexList.add(vertexList.size(), vertex);
    }

    //插入结点
    public void insertEdge(int v1, int v2, int weight) {
        edges[v1][v2] = weight;
        numOfEdges++;
    }

    //删除结点
    public void deleteEdge(int v1, int v2) {
        edges[v1][v2] = 0;
        numOfEdges--;
    }

    //得到第一个邻接结点的下标
    public int getFirstNeighbor(int index) {
        for(int j = 0; j < vertexList.size(); j++) {
            if (edges[index][j] > 0) {
                return j;
            }
        }
        return -1;
    }

    //根据前一个邻接结点的下标来取得下一个邻接结点
    public int getNextNeighbor(int v1,int v2) {
        for (int j = v2 + 1; j < vertexList.size(); j++) {
            if (edges[v1][j] > 0) {
                return j;
            }
        }
        return -1;
    }
    
    //私有函数，深度优先遍历
    private void DFS(boolean[] isVisited, int i) {
        //首先访问该结点，在控制台打印出来
        System.out.print(getValueByIndex(i)+"  ");
        //置该结点为已访问
        isVisited[i] = true;
        
        int w = getFirstNeighbor(i);//访问i结点的第一个邻居
        while (w != -1) {
            if (!isVisited[w]) {
            	DFS(isVisited, w);
            }
            w = getNextNeighbor(i, w);
        }
    }
    
    //对外公开函数，深度优先遍历
    public void depthFirstSearch() {
        for(int i = 0; i < getNumOfVertex(); i++) {
            //因为对于非连通图来说，并不是通过一个结点就一定可以遍历所有结点的。
            if (!isVisited[i]) {
            	DFS(isVisited, i);
            }
        }
    }
    
    //私有函数，广度优先遍历
    private void BFS(boolean[] isVisited, int i) {
        int u, w;
        LinkedList<Integer> queue = new LinkedList<Integer>();
        
        //访问结点i
        System.out.print(getValueByIndex(i) + "  ");
        isVisited[i] = true;
        //结点入队列
        queue.addLast(i);
        while (!queue.isEmpty()) {
            u = ((Integer)queue.removeFirst()).intValue();
            w = getFirstNeighbor(u);
            while(w!=-1) {
                if(!isVisited[w]) {
                        //访问该结点
                        System.out.print(getValueByIndex(w) + "  ");
                        //标记已被访问
                        isVisited[w] = true;
                        //入队列
                        queue.addLast(w);
                }
                //寻找下一个邻接结点
                w=getNextNeighbor(u, w);
            }
        }
    }    
    
    //对外公开函数，广度优先遍历
    public void broadFirstSearch() {
        for(int i = 0; i < getNumOfVertex(); i++) {
            if(!isVisited[i]) {
                BFS(isVisited, i);
            }
        }
    }   
    
    public static void main(String args[]) {
        int n = 8, e = 9;//分别代表结点个数和边的数目，e可以省略
        String labels[]={"1","2","3","4","5","6","7","8"};//结点的标识
        GraphTraverse graph = new GraphTraverse(n);
        for(String label:labels) {
            graph.insertVertex(label);//插入结点
        }
        //插入九条边
        graph.insertEdge(0, 1, 1);
        graph.insertEdge(0, 2, 1);
        graph.insertEdge(1, 3, 1);
        graph.insertEdge(1, 4, 1);
        graph.insertEdge(3, 7, 1);
        graph.insertEdge(4, 7, 1);
        graph.insertEdge(2, 5, 1);
        graph.insertEdge(2, 6, 1);
        graph.insertEdge(5, 6, 1);
        graph.insertEdge(1, 0, 1);
        graph.insertEdge(2, 0, 1);
        graph.insertEdge(3, 1, 1);
        graph.insertEdge(4, 1, 1);
        graph.insertEdge(7, 3, 1);
        graph.insertEdge(7, 4, 1);
        graph.insertEdge(6, 2, 1);
        graph.insertEdge(5, 2, 1);
        graph.insertEdge(6, 5, 1);
        
        graph.initIsVisited();
        System.out.println("深度优先搜索序列为：");
        graph.depthFirstSearch();
        System.out.println();
        
        graph.initIsVisited();
        System.out.println("广度优先搜索序列为：");
        graph.broadFirstSearch();
    }
}