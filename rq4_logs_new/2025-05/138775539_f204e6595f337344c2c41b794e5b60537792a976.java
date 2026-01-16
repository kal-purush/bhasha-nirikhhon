package my.com.Matrix;

import java.util.* ;

// Shortest Path in Binary Matrix
// Given an n x n binary matrix grid, return the length of the shortest clear path in the matrix.
// If there is no clear path, return -1.

// Approach: Use BFS to traverse the matrix, marking visited cells and counting the distance.
// A clear path in the binary matrix is a path from the top-left corner (0, 0) to the bottom-right corner (n-1, n-1) such that all the visited cells are 0.

class Solution {
    
    public int shortestPathBinaryMatrix(int[][] grid) {
        
        int len = grid.length;
        if (len == 0)
            return -1;
        if ( grid[0][0] == 1 || grid[len-1][len-1] == 1)
            return -1;
        // use a queue to store the next position of path by using formula : i*r+c
        // check for corner case for 8 directional cells and add to the path.
     
        ArrayDeque<int[]> queue = new ArrayDeque();
        
        int[][] operations = { {-1,-1},{-1,0},{-1,+1},{0,-1},{0,1},{1,-1},{1,0},{1,1}};
        
        queue.add( new int[]{0, 0});
        
        grid[0][0] = 1; // set the distance on the grid , traverse to the nearest zero's and add 1 to the distance 
        while( !queue.isEmpty()){
            
            //int size = queue.size();
            //for( int i=0 ; i<size; i++){
                
                int[] n = queue.poll();
                if ( n[0] == len-1 && n[1] == len-1 ){
                    return grid[n[0]][n[1]];
                }
                for ( int[] op : operations){

                    int r = n[0] + op[0];
                    int c = n[1] + op[1];
                    int d = grid[n[0]][n[1]];
                    if ( r<0 || r>=len || c<0 || c>=len || grid[r][c] != 0 ){
                        // do not add to the queue 
                        continue;
                    } else {
                        //and increase the distance for the visited cell in the grid
                        queue.add( new int[]{ r, c });
                        grid[r][c] = d + 1;
                    }
                    
                }
                // reset the last visited cell to 1
           // }
        }
        
        return -1;
    }
}