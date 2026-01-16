class CourseSchedule {
    public boolean canFinish(int numCourses, int[][] prerequisites) {
        //build graph
        //Check if its a directed acyclic graph, if yes, return true or false;

        Map<Integer,List<Integer>> graph = new HashMap<>();
        int[] visited = new int[numCourses];

        //add nodes;
        for(int i = 0; i<numCourses; i++){
            graph.put(i,new ArrayList<>());
        }

        //add edges;
        for(int[] edge : prerequisites){
            graph.get(edge[1]).add(edge[0]);
        }
        //start dfs at any numCourses
        for(int i = 0; i<numCourses; i++){
            if(isCyclic(graph,visited,i)){
                return false;
            }
        }
        return true;
    }

    boolean isCyclic(Map<Integer,List<Integer>> graph,int[] visited, int i){
        if(visited[i] == 1)
            return true;
        if(visited[i] == 2)
            return false;

        visited[i] = 1;
        for(int n : graph.get(i)){
            if(isCyclic(graph,visited,n)){
                return true;
            }
        }
        visited[i] = 2;

        return false;
    }
}

//intuition: find cycle in a directed graph, if found return false, else return true.
// used simple dfs logic. each node is assinged 0 - not visited, 1- currently visiting, 2- visited.
// return true for a cycle if we encounter same node again. i.e, when we encounter visited[i] == 1;
// Time Complexity : O(v+e) for dfs. in this case O(numCourses+prerequiaites.length)
// Space Complexity :O(n) to store n elements. and dfs goes max depth O(h) wherhe h <n;