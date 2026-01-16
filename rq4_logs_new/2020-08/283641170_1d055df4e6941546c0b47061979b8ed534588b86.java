class NoOfIslands {
    public int numIslands(char[][] grid) {

        if(grid==null || grid.length==0)
            return 0;
        int m = grid.length;
        int n = grid[0].length;
        int count = 0;

        for(int i = 0; i<m;i++){
            for(int j =0; j<n;j++){
                if(grid[i][j] == '1'){
                    count++;
                    dfs(grid,m,n,i,j);
                }
            }
        }
        return count;
    }
    void dfs(char[][] grid,int m, int n, int i, int j){
        if(grid[i][j]=='0')
            return;

        grid[i][j] ='0';
        int[][] directions = {{0,1},{0,-1},{1,0},{-1,0}};

        for(int[] dir:directions){
            int r = i+dir[0];
            int c = j+dir[1];
            if(r>=0 && c>=0 && r<m && c<n){
                dfs(grid,m,n,r,c);
            }
        }
    }
}
//time complexity : O(V+E)
// Common dfs question with grid type graph. traverse in all directions and use dfs