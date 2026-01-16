package pl.put.poznan.networkanalyzer.strategy;

import pl.put.poznan.networkanalyzer.service.ConnectionService;
import pl.put.poznan.networkanalyzer.service.NodeService;

import java.util.*;

public class BFS implements ShortestPathStrategy{

    private NodeService nodeService;
    private ConnectionService connectionService;

    public BFS(NodeService nodeService, ConnectionService connectionService) {
        this.nodeService = nodeService;
        this.connectionService = connectionService;
    }


    public ArrayList<Integer> getShortestPath(int source, int dest){
        ArrayList<Integer> shortestPathList = new ArrayList<Integer>();
        HashMap<Integer, Boolean> visited = new HashMap<Integer, Boolean>();

        if (source == dest)
            return null;
        Queue<Integer> queue = new LinkedList<>();
        Stack<Integer> pathStack = new Stack<>();

        queue.add(source);
        pathStack.add(source);
        visited.put(source, true);

        while(!queue.isEmpty()){
            int u = queue.poll();
            List<Integer> adjList = nodeService.getOneNode(u).getOutgoing();

            for(int v:adjList){
                if(!visited.containsKey(v)){
                    queue.add(v);
                    visited.put(v,true);
                    pathStack.add(v);
                    if(u==dest) break;
                }
            }
        }

        int node, currentSrc = dest;
        shortestPathList.add(dest);
        while (!pathStack.isEmpty()){
            node = pathStack.pop();
            if(connectionService.getConnectionByNodes(node,currentSrc).getValue()>0){
                shortestPathList.add(node);
                currentSrc = node;
                if(node == source) break;
            }
        }
        return shortestPathList;
    }


}