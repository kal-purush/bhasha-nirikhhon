package leetcode;

public class UniquePaths2 {
	public int uniquePathsWithObstacles(int[][] obstacleGrid) {
        int m = obstacleGrid.length;
        int n = obstacleGrid[0].length;
        if(m == 0 || n == 0) return 0;
        if(obstacleGrid[m-1][n-1] == 1 || obstacleGrid[0][0] == 1) return 0;
        int[][] result = new int[m][n];
        
        boolean block1 = false;
        boolean block2 = false;
        result[0][0] = 1;
        for(int i = 1; i < m; i++){
            if(obstacleGrid[i][0] == 1) block1 = true;
            result[i][0] = block1 ? 0: 1;
        }
        for(int j = 1; j < n; j++){
            if(obstacleGrid[0][j] == 1) block2 = true;
            result[0][j] = block2 ? 0: 1;
        }
        
        for(int x = 1; x < m; x++){
            for(int y = 1; y < n; y++){
                if(obstacleGrid[x][y] == 1){
                    result[x][y] = 0;  
                }else{
                    result[x][y] = result[x-1][y] + result[x][y-1];
                } 
            }
        }
        
        return result[m-1][n-1];
    }
}