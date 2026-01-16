package touro.ConwaysGameOfLife;


public class Grid {

    private boolean[][] board;
    private  Direction[] directions = Direction.values();

    private int height;
    private int width;

    private boolean isAlive = true;

    public Grid(int width, int height) {
        this.height = height;
        this.width = width;
        this.board = new boolean[width][height];
    }


    public void makeMove() {
        boolean[][] tempBoard = new boolean[width][height];

        for (int y = 0; y < height; y++) {
            for (int x = 0; x < width; x++) {
                int aliveNeighbors = countAliveNeighbors(x, y);

                if (checkStatus(x, y)) {
                    if (aliveNeighbors < 2) {
                        setDead(x,y);
                    } else if (aliveNeighbors == 2 || aliveNeighbors == 3) {
                        setAlive(x,y);
                    } else if (aliveNeighbors > 3) {
                        setDead(x,y);
                    } else {
                        if (aliveNeighbors == 3) {
                            setAlive(x,y);
                        }
                    }
                }
            }
        }
        this.board = tempBoard;
    }

    public int countAliveNeighbors(int x, int y) {
        int aliveNeighborCount = 0;
        for(Direction d : directions) {
            boolean isAlive = checkDirection(d, x, y);      //check neighbor status


            if(isAlive){           //if neighbor cell is has true val, increment live neighbors
                aliveNeighborCount++;
            }
        }
        return aliveNeighborCount;
    }

    public boolean checkStatus(int x, int y) {
        //make sure in bounds
        if (x < 0 || x >= width){
            isAlive = false;
        }
        if(y < 0 || y > height){
            isAlive = false;
        }
        return isAlive;
    }


    public boolean isAlive(int x, int y){
        return isAlive;
    }

    public void setAlive(int x, int y){
        // isAlive = true;
        this.board[x][y] = true;
    }

    /**
     * @return value of cell is 0 - dead
     */
    public void setDead(int x, int y){
        //isAlive = false;
        this.board[x][y] = false;
    }


/*    public List<Cell> getBoard(){
        return boardCells;
    }*/


    public boolean checkDirection(Direction direction, int x, int y) {
        switch (direction) {
            case North:
                boolean inBounds = checkStatus(x, y+1);
                if(!inBounds){
                    return false;
                }
                else{
                    return this.board[x][y+1];
                }

            case East:
                inBounds = checkStatus(x + 1, y);
                if(!inBounds){
                    return false;
                }
                else{
                    return this.board[x+1][y];
                }

            case South:
                inBounds = checkStatus(x, y-1);
                if(!inBounds){
                    return false;
                }
                else{
                    return this.board[x][y-1];
                }

            case West:
                inBounds = checkStatus(x - 1, y);
                if(!inBounds){
                    return false;
                }
                else{
                    return this.board[x -1][y];
                }

            case NorthEast:
                inBounds = checkStatus(x + 1, y + 1);
                if(!inBounds){
                    return false;
                }
                else{
                    return this.board[x + 1][y + 1];
                }
            case NorthWest:
                inBounds = checkStatus(x - 1, y + 1);
                if(!inBounds){
                    return false;
                }
                else{
                    return this.board[x - 1][y + 1];
                }
            case SouthEast:
                inBounds = checkStatus(x + 1, y-1);
                if(!inBounds){
                    return false;
                }
                else{
                    return this.board[x + 1][y-1];
                }
            case SouthWest:
                inBounds = checkStatus(x - 1, y-1);
                if(!inBounds){
                    return false;
                }
                else{
                    return this.board[x - 1][y-1];
                }
            default:
                throw new RuntimeException(direction + " is not a known touro.ConwaysGameOfLife.Direction");
        }
    }

}