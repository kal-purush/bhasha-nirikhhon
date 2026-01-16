/*
        In a given grid, each cell can have one of three values:
        the value 0 representing an empty cell;
        the value 1 representing a fresh orange;
        the value 2 representing a rotten orange.
        Every minute, any fresh orange that is adjacent (4-directionally) to a rotten orange becomes rotten.

        Return the minimum number of minutes that must elapse until no cell has a fresh orange.  If this is impossible, return -1 instead.

        Example 1:
        Input: [[2,1,1],[1,1,0],[0,1,1]]
        Output: 4

        Example 2:
        Input: [[2,1,1],[0,1,1],[1,0,1]]
        Output: -1
        Explanation:  The orange in the bottom left corner (row 2, column 0) is never rotten, because rotting only happens 4-directionally.

        Example 3:
        Input: [[0,2]]
        Output: 0
        Explanation:  Since there are already no fresh oranges at minute 0, the answer is just 0.
        
        Note:

        1 <= grid.length <= 10
        1 <= grid[0].length <= 10
        grid[i][j] is only 0, 1, or 2.
*/
class Solution {
        class Point{
                public int row;
                public int col;
                
                public Point(int row, int col){
                        this.row = row;
                        this.col = col;
                }
                
                public String toString(){
                        return " r :"+ this.row+ " , col: "+this.col;
                }
        }
        public int orangesRotting(int[][] grid) {
                Queue<Point> q = new LinkedList();
                
                for(int i = 0 ; i < grid.length; i++){
                        for(int j = 0; j < grid[0].length ; j++){
                                if(grid[i][j] == 2){
                                        q.add(new Point(i, j));
                                }
                        }
                }
                
                int time = 0;
                while(!q.isEmpty()){
                        // System.out.println("-----");
                        // print(q);
                        int size = q.size();
                        time++;
                        for(int i = 0; i < size; i++){
                                Point p     = q.poll();
                                Point tp    = top(p, grid);
                                if(tp != null){
                                        q.add(tp);
                                        grid[tp.row][tp.col] = 2;
                                }
                                
                                Point bp = bottom(p, grid);
                                if(bp != null){
                                        q.add(bp);
                                        grid[bp.row][bp.col] = 2;
                                }
                                
                                Point lp = left(p, grid);
                                if(lp != null){
                                        q.add(lp);
                                        grid[lp.row][lp.col] = 2;
                                }
                                
                                Point rp = right(p, grid);
                                if(rp != null){
                                        q.add(rp);
                                        grid[rp.row][rp.col] = 2;
                                }
                        }
                }
                
                return check(grid) ? -1 : time == 0 ? 0 : time-1;
        }
        
        private void print(Queue<Point> q){
                q.forEach(System.out::println);
        }
        
        private boolean check(int[][] grid){
                for(int i = 0 ; i < grid.length; i++){
                        for(int j = 0; j < grid[0].length; j++){
                                if(grid[i][j] == 1){
                                        return true;
                                }
                        }
                }
                return false;
        }
        
        private Point top(Point p, int[][] grid){
                if(p.row > 0 && grid[p.row-1][p.col] == 1){
                        return new Point(p.row - 1, p.col);
                }
                return null;
        }
        
        private Point bottom(Point p, int[][] grid){
                if(p.row + 1 < grid.length && grid[p.row+1][p.col] == 1){
                        return new Point(p.row + 1, p.col);
                }
                return null;
        }
        
        private Point left(Point p, int[][] grid){
                if(p.col > 0 && grid[p.row][p.col-1] == 1){
                        return new Point(p.row, p.col - 1);
                }
                return null;
        }
        
        private Point right(Point p, int[][] grid){
                if(p.col + 1 <  grid[0].length && grid[p.row][p.col+1] == 1){
                        return new Point(p.row, p.col + 1);
                }
                return null;
        }
}