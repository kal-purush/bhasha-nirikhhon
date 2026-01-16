class Solution {
    public boolean canFinish(int numCourses, int[][] prerequisites) {
        

        //Basically implementing the topological sort on this, if there is a topological order then it's a true else false

        //Algorithm: 
        //1. Form a graph 
        List<List<Integer>> adj=new ArrayList<>();
        for(int i=0; i<numCourses; i++){
            adj.add(new ArrayList<>());
        }

        //2. Calculate the indegree of nodes
        int indegree[]=new int[numCourses];
        for(int[] p: prerequisites){
            int u=p[1];
            int v=p[0];

            adj.get(u).add(v);
            indegree[v]++;
        }

        Queue<Integer> queue=new LinkedList<>();
        //3. Initialize the queue with the nodes with indegreee asa 0
        for(int i=0;i <numCourses; i++){
            if(indegree[i] == 0){
                queue.add(i);
            }
        }

        List<Integer> topo=new ArrayList<>();

        /*
        Process the queue
         */
        while(!queue.isEmpty()){
            int c=queue.poll();
            topo.add(c);

            for(int nei: adj.get(c)){
                indegree[nei]--;
                if(indegree[nei]==0) queue.add(nei);
            }
        }

    return topo.size()==numCourses;
    }
}