public class Board {

    // Save the board as a 2D array of Piece objects
    Piece[][] theBoard = null;

    // Save the depth of each column of the board for every given point in time
    int[] boardMap = null;

    /**
     * Initialize the board by creating the Piece matrix and the boardMap
     */
    public Board() {
        theBoard = initBoard();
        boardMap = initBoardMap();
    }

    /**
     * @return a Piece[][] representing the matrix of pieces
     * Board is hard coded in first release for a standard 6x7 board
     */
    public Piece[][] initBoard() {

        // Hard code 6x7 board for first release
        Piece[][] tempBoard = new Piece[6][7];
        
        for(int i = 0; i < tempBoard.length; i++) {
            for(int j = 0; j < tempBoard[i].length; j++) {

                // Create an unclaimed piece at each board position
                tempBoard[i][j] = new Piece(0);
            }
        }

        return tempBoard;
    }

    /**
     * @return an int[] representing the depth of each column on the board at a given point in time
     * Board map is hard coded in first release for a 6x7 board
     */
    public int[] initBoardMap() {

        // Initially hard coded for first release 
        int[] tempBoardMap = new int[7];

        // Hard code the initial depth of each column to its lowest valid position
        for(int i = 0; i < tempBoardMap.length; i++) {
            tempBoardMap[i] = tempBoardMap.length - 2;
        }

        return tempBoardMap;
    }

    /**
     * Let the board object print itself with the current state of the game
     */
    public void printBoard() {

        // Print the headers for each column 
        for(int i = 1; i < 8; i++) {

            // Debugging
            // System.out.print(" " + i + "  ");
            System.out.print(i + " ");
        }

        System.out.println();

        // Print the 6x7 board showing the state of each piece
        for(int i = 0; i < theBoard.length; i++) {
            for(int j = 0; j < theBoard[i].length; j++) {

                // If space is empty, print a point
                if(theBoard[i][j].player == 0) {

                    //Debugging
                    // System.out.print(" .," + theBoard[i][j].getMaxNumConnected());
                    System.out.print(". " );
                } 
                
                // If Player 1 placed a piece in a given spot, print an X
                else if(theBoard[i][j].player == 1) {

                    //Debugging
                    // System.out.print(" X," + theBoard[i][j].getMaxNumConnected());
                    System.out.print("X ");
                } 
                
                // If Player 2 placed a piece in a given spot, print an O
                else if(theBoard[i][j].player == -1) {

                    //Debugging
                    // System.out.print(" O," + theBoard[i][j].getMaxNumConnected());
                    System.out.print("O ");
                }
            }
            System.out.println();
        }

    }


}