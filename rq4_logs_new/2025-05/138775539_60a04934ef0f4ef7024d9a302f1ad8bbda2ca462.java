class Solution {
    
    int mincount = 0 ;
    public int orangesRotting(int[][] grid) {
        
       
        int m =grid.length , n = grid[0].length;
        if (m == 1 && n==1 && grid[0][0]==1) {
            return -1;
        }
        if (m == 1 && n==1) {
            return 0;
        } 
         
        int r = -1 , c = -1;

        ArrayDeque<Integer> que = new ArrayDeque();

        for(int i=0 ; i < m ; i++ ){
            for(int j=0 ; j < n ; j++ ){
                if(grid[ i ] [ j ]  == 2){
                    int v = i*n +j;
                    que.add(v);
                }
            }
        }

        while (!que.isEmpty()) { 
            
            int size = que.size();
            
            for (int i=0 ; i < size ;i++ ){
                int idx = que.pop();
                r = idx/n;
                c = idx%n;

                if( r+1 < m && grid[r+1][c]  == 1 ) {
                    que.add( (r+1)*n + c );
                    grid[r+1][c]  = 2;
                }

                if( r-1 >= 0 && grid[r-1][c]  == 1 ){
                    que.add( (r-1)*n + c );
                    grid[r-1][c]  = 2;
                }
                    
                if( c+1 < n && grid[r][c+1]  == 1 ){
                    que.add( r*n + (c+1) );
                    grid[r][c+1]  = 2;
                }

                if( c-1 >= 0 && grid[r][c-1]  == 1 ){
                    que.add( r*n + (c-1) );
                        grid[r][c-1]  = 2;
                }
            }

            if(!que.isEmpty())       
                mincount++;
        }
        

        

        // check if there are any 1 in the array
        for(int i=0 ; i < m ; i++ ){
            for(int j=0 ; j < n ; j++ ){
                if(grid[ i ] [ j ]  == 1){
                    return -1;
                }
            }
        }

        if ( r == -1 || c == -1 )
        return 0;


        return mincount;
    }

    public void helper ( int r , int c , int[][] grid){

        if (r < 0 || r == grid.length)
            return;
        
        if (c < 0 || c == grid[0].length)
            return;

        if (grid[r][c] == 0)
        return;
        if (grid[r][c] == 1) {
            grid[r][c] = 2;
        }
        
        if (grid[r][c] == 2 ){
            
            if( ( r+1 < grid.length && grid[r+1][c] == 1)
            || ( r-1 >0 && grid[r-1][c] == 1 )
            || ( c+1 < grid[0].length && grid[r][c+1] == 1)
            || ( c-1 > 0 && grid[r][c-1] == 1 ) ) {
                mincount++ ;
            }
            
            if ( r+1 < grid.length && grid[r+1][c] == 1) {
                helper ( r+1 , c , grid);
            }
            if ( r-1 >0 && grid[r-1][c] == 1 ) {
                helper ( r-1, c , grid);
            }
            if ( c+1 < grid[0].length && grid[r][c+1] == 1) {
                helper ( r , c+1 , grid);
            }
            if ( c-1 > 0 && grid[r][c-1] == 1) {
                helper ( r , c-1 , grid);
            }
        
        /*    helper ( r+1 , c , grid);
            helper ( r-1, c , grid);
            helper ( r , c+1 , grid);
            helper ( r , c-1 , grid);
        */
        }
    }
}