package rebuild;


public class Board {
    private Tile[][] board;
    private int diff;
    
    public Board(int length,int width, int difficulty) {
	board = new Tile[length][width];
	diff = difficulty;
	init();
    }
    
    public void init() {
	for(int i = 0; i < board.length; i++) {
	    for(int j = 0; j < board[0].length; j++) {
		board[i][j] = new Tile(diff);
	    }
	}
	
	spawnCheckpoint();
    }
    
    public void spawnCheckpoint() {
	int x; int y;
	
	if(board.length%2 == 0) {
	    x = board.length/2;
	} else {
	    x = (board.length-1)/2;
	}
	
	if(board[0].length%2 == 0) {
	    y = board[0].length/2;
	} else {
	    y = (board[0].length-1)/2;
	}
	
	board[x][y].setType(TyleType.Checkpoint);
    }
  
    public Tile[][] getTiles() {
	return board;
    }
    
    public Tile getTile(int[] coordinate) {
	return board[coordinate[0]][coordinate[1]];
    }

    public void putRobotOn(int[] coordinate) {
	board[coordinate[0]][coordinate[1]].setType(TyleType.Floor);
    }
    
}