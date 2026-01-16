class Solution {
    public int findAnswer(int row,int col,int grid[][],int m,int n,int dp[][]){
        if(row>=m || col>=n || row<0 || col<0){
            return 0;
        }

        if(dp[row][col]!=-1)
        return dp[row][col];

        int upD = 0;int downD = 0;int right = 0;
        if(row>0 && col<n-1 && grid[row-1][col+1]>grid[row][col]){
            upD = 1 + findAnswer(row-1,col+1,grid,m,n,dp);
        }
        if(col<n-1 && grid[row][col+1]>grid[row][col]){
            right = 1 + findAnswer(row,col+1,grid,m,n,dp);
        }
        if(row<m-1 && col<n-1 && grid[row+1][col+1]>grid[row][col]){
            downD = 1 + findAnswer(row+1,col+1,grid,m,n,dp);
        }

        return dp[row][col] = Math.max(Math.max(upD,downD),right);
    }
    public int maxMoves(int[][] grid) {
        int ans = Integer.MIN_VALUE;
        int m = grid.length;
        int n = grid[0].length;
        int dp[][] = new int[m+1][n+1];
        for(int row[]:dp){
            Arrays.fill(row,-1);
        }
        for(int i = 0;i<m;i++){
            ans = Math.max(ans,findAnswer(i,0,grid,m,n,dp));
        }
        return ans;
    }
}