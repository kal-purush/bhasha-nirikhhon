class OrangesRotting {
    public int orangesRotting(int[][] grid) {
        //coroner cases
        if(grid==null || grid[0].length==0)
            return 0;

        int m = grid.length, n = grid[0].length, freshoranges = 0, minMinutes =0;
        Queue<int[]> queue = new LinkedList();
        Queue<Integer> level = new LinkedList();
        int[][] directions = {{0,1},{0,-1},{1,0},{-1,0}};

        for(int i = 0;i<m;i++){
            for(int j = 0; j<n; j++){
                if(grid[i][j] == 2){
                    queue.offer(new int[]{i,j});
                    level.offer(0);
                }
                if(grid[i][j] == 1){freshoranges++;}
            }
        }

        while(!queue.isEmpty()){
            int[] node = queue.poll();
            int l = level.poll();
            minMinutes = Math.max(l,minMinutes);
            for(int[] dir : directions){
                int r = node[0]+dir[0];
                int c = node[1]+dir[1];

                if(r>=0 && r<m && c>=0 && c<n && grid[r][c] == 1){
                    grid[r][c] = 2;
                    freshoranges--;
                    queue.offer(new int[]{r,c});
                    level.offer(l+1);
                }
            }
        }
        return (freshoranges==0)?minMinutes:-1;
    }
}