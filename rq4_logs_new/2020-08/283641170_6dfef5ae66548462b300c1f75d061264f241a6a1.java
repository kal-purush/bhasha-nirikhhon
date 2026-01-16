class WordSearchII {
    public List<String> findWords(char[][] board, String[] words) {
        if(words.length==0 || board.length ==0 || board == null)
            return new ArrayList();

        Set<String> ans = new HashSet<>();
        int m = board.length;
        int n = board[0].length;
        Arrays.sort(words);
        for(int k = 0; k<words.length; k++) {
            if(k>0 && words[k].equals(words[k-1])){continue;}
            String word = words[k];
            boolean[][] visited = new boolean[m][n];
            int index =0;
            for(int i =0; i<m; i++){
                for(int j =0; j<n; j++){
                    if(board[i][j] == word.charAt(index)){
                        if(findWords(board,visited,word,i,j,m,n,index+1)){
                            ans.add(word);
                        }
                    }
                }
            }
        }
        return new ArrayList<>(ans);
    }
    boolean findWords(char[][] board, boolean[][] visited, String word, int i , int j, int m, int n, int index){
        if(visited[i][j] == true)
            return false;
        if(index == word.length())
            return true;
        visited[i][j] = true;
        int[][] directions = {{-1,0},{1,0},{0,1},{0,-1}};

        for(int[] dir: directions){
            int r = i+dir[0];
            int c = j+dir[1];

            if(r>=0 && r<m && c>=0 && c<n && board[r][c] == word.charAt(index)){
                if(findWords(board,visited,word,r,c,m,n,index+1))
                    return true;
            }
        }
        visited[i][j] = false;
        return false;
    }
}