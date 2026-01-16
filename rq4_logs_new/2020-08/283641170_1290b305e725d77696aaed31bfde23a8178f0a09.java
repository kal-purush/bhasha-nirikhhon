public class ZombieInMatrix{
    public int minHours(int[][] grid){
        if(grid == null || grid.length == 0)
            return 0;
        int m = 0, n = 0, humans = 0,minhours = 0;
        Queue<int[]> queue = new LinkedList();
        Queue<Integer> level = new LinkedList();
        int[][] directions = {{0,1},{1,0},{-1,0},{0,-1}};

        for(int i = 0; i<m; i++){
            for(int j = 0; j<n; j++){
                if(grid[i][j] == 1){
                    queue.offer(new int[]{i,j});
                    level.offer(0);
                }
                if(grid[i][j] == 0){humans++;}
            }
        }

        while(!queue.isEmpty()){
            int[] node = queue.poll();
            int l = level.poll();
            minhours = Math.max(minhours,l);
            for(int[] dir:directions){
                int r = node[0]+dir[0];
                int c = node[1]+dir[1];
                if(r >=0 && r<m && c>=0 && c<n && grid[r][c] == 0){
                    grid[r][c] = 1;
                    queue.offer(new int[]{r,c});
                    level.offer(l+1);
                    humans--;
                }
            }
        }
        return (humans>0)?-1:minHours;
    }
}