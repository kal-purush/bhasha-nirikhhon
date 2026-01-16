package my.com.app;

// iterate over all paths from source to target
// 0 to n-1
// use DFS to find all paths
// use a list to store the path
// when we reach the target, add the path to the result
// remove the last element from the path when backtracking
// use a list of lists to store the result


class Solution {
    public List<List<Integer>> allPathsSourceTarget(int[][] graph) {
        
        List<List<Integer>> res = new ArrayList();
        
        List<Integer> l = new ArrayList();
        l.add(0);
        dfs( res, graph , 0 ,l );
        return res;
    }
    
    public void dfs(List<List<Integer>> res, int[][] gh, int source, List<Integer> path){
        
        
        if ( source == gh.length -1){
            res.add(new ArrayList(path));
            return;
        }
        
        for ( int i : gh[source] ){
            path.add(i);
            dfs(res , gh, i , path );
            path.remove(path.size()-1);
        }
    }
}