package Game;

public class BoardStateCalc {

    /**
     * Computes the length of the horizontal sequence at the current position  
     */
    public static void computeHorizontalAxis(Board board, int col, boolean p1Turn) {

        int left = 0;
        int right = 0;

        // Get the length of the sequence on the left
        try {

            // Determines if looking for pieces for player 1 or 2
            if(p1Turn) {
                left = board.theBoard[(board.getBoardMap()[col - 1])][col - 2].getPlayer() == 1 ? board.theBoard[(board.getBoardMap()[col - 1])][col - 2].getHorizontalNumConnected() : 0;
            } else {
                left = board.theBoard[(board.getBoardMap()[col - 1])][col - 2].getPlayer() == -1 ? board.theBoard[(board.getBoardMap()[col - 1])][col - 2].getHorizontalNumConnected() : 0;
            }
        } catch(Exception e) {
            left = 0;
        }

        // Get the length of the sequence on the right
        try {
            if(p1Turn) {
                right = board.theBoard[(board.getBoardMap()[col - 1])][col].getPlayer() == 1 ? board.theBoard[(board.getBoardMap()[col - 1])][col].getHorizontalNumConnected() : 0;
            } else {
                right = board.theBoard[(board.getBoardMap()[col - 1])][col].getPlayer() == -1 ? board.theBoard[(board.getBoardMap()[col - 1])][col].getHorizontalNumConnected() : 0;
            }
        } catch(Exception e) {
            right = 0;
        }

        // The length of the horizontal sequence at the current position is equal to the length of the left sequence plus the right sequence plus 1 (for the current piece)
        board.theBoard[(board.getBoardMap()[col - 1])][col - 1].setHorizontalNumConnected(Math.max(left + right + 1, 1));

        propagateValueHorizontally(board, board.theBoard[(board.getBoardMap()[col - 1])][col - 1].getPlayer(), 
        board.theBoard[(board.getBoardMap()[col - 1])][col - 1].getHorizontalNumConnected(), 
        left,
        right,
        col - 1);

    }

    /**
     * Updates all pieces in a horizontal sequence with the full sequence length
     */
    public static void propagateValueHorizontally(Board board, int player, int value, int left, int right, int colIndex) {

        // Loop while there are still values to update either to the left or right of the piece placed
        for(int i = 0; i < left || i < right; i++) {

            // If there is a piece on the left, update it
            try {
                if(board.theBoard[board.getBoardMap()[colIndex]][colIndex - (i + 1)].getPlayer() == player && i < left) {
                    board.theBoard[board.getBoardMap()[colIndex]][colIndex - (i + 1)].setHorizontalNumConnected(value);
                    // System.out.println("Horizontal sequence length at position [" + colIndex + "][" + (colIndex - (i + 1)) + "] is set to " + board.theBoard[board.getBoardMap()[colIndex]][colIndex - (i + 1)].getHorizontalNumConnected());
                }
            } catch(Exception e) {
                left = -1;
            }

            // If there is a piece on the right, update it 
            try {
                if(board.theBoard[board.getBoardMap()[colIndex]][colIndex + (i + 1)].getPlayer() == player && i < right){
                    board.theBoard[board.getBoardMap()[colIndex]][colIndex + (i + 1)].setHorizontalNumConnected(value);
                    // System.out.println("Horizontal sequence length at position [" + colIndex + "][" + (colIndex + (i + 1)) + "] is set to " + board.theBoard[board.getBoardMap()[colIndex]][colIndex + (i + 1)].getHorizontalNumConnected());
                }
            } catch(Exception e) {
                right = -1;
            }
        }
    }

    /**
     * Computes the length of the vertical sequence at the current position  
     */
    public static void computeVerticalAxis(Board board, int col, boolean p1Turn) {

        int left = 0;
        int right = 0;

        // Get the length for the sequence above the current position
        try {

            // Unique checks for player 1 and player 2
            if(p1Turn) {
                left = board.theBoard[(board.getBoardMap()[col - 1]) - 1][col - 1].getPlayer() == 1 ? board.theBoard[(board.getBoardMap()[col - 1]) - 1][col - 1].getVerticalNumConnected() : 0;
            } else {
                left = board.theBoard[(board.getBoardMap()[col - 1]) - 1][col - 1].getPlayer() == -1 ? board.theBoard[(board.getBoardMap()[col - 1]) - 1][col - 1].getVerticalNumConnected() : 0;
            }
        } catch(Exception e) {
            left = 0;
        }

        // Get the length for the sequence below the current position
        try {

            // Unique checks for player 1 and player 2
            if(p1Turn) {
                right = board.theBoard[(board.getBoardMap()[col - 1]) + 1][col - 1].getPlayer() == 1 ? board.theBoard[(board.getBoardMap()[col - 1]) + 1][col - 1].getVerticalNumConnected() : 0;
            } else {
                right = board.theBoard[(board.getBoardMap()[col - 1]) + 1][col - 1].getPlayer() == -1 ? board.theBoard[(board.getBoardMap()[col - 1]) + 1][col - 1].getVerticalNumConnected() : 0;
            }
        } catch(Exception e) {
            right = 0;
        }

        // The length of the vertical sequence at the current position is equal to the length of the left sequence plus the right sequence plus 1 (for the current piece)
        board.theBoard[(board.getBoardMap()[col - 1])][col - 1].setVerticalNumConnected(Math.max(left + right + 1, 1));

        propagateValueVertically(board, board.theBoard[(board.getBoardMap()[col - 1])][col - 1].getPlayer(), 
        board.theBoard[(board.getBoardMap()[col - 1])][col - 1].getVerticalNumConnected(), 
        left,
        right,
        col - 1);

    }

    /**
     * Updates all pieces in a vertical sequence with the full sequence length
     */
    public static void propagateValueVertically(Board board, int player, int value, int above, int below, int colIndex) {

        // Loop while there are still values to update either above or below the piece placed
        for(int i = 0; i < above || i < below; i++) {

            // If there is a piece above, update it
            try {
                if(board.theBoard[(board.getBoardMap()[colIndex]) - (i + 1)][colIndex].getPlayer() == player && i < above) {
                    board.theBoard[(board.getBoardMap()[colIndex]) - (i + 1)][colIndex].setVerticalNumConnected(value);
                    // System.out.println("Vertical sequence length at position [" + ((board.getBoardMap()[colIndex]) - (i + 1)) + "][" + colIndex + "] is set to " + board.theBoard[board.getBoardMap()[colIndex]][colIndex - (i + 1)].getVerticalNumConnected());
                }
            }
            catch(Exception e) {
                above = -1;
            }

            // If there is a piece below, update it 
            try { 
                if(board.theBoard[(board.getBoardMap()[colIndex]) + (i + 1)][colIndex].getPlayer() == player && i < below) {
                    board.theBoard[(board.getBoardMap()[colIndex]) + (i + 1)][colIndex].setVerticalNumConnected(value);
                    // System.out.println("Vertical sequence length at position [" + ((board.getBoardMap()[colIndex]) + (i + 1)) + "][" + colIndex + "] is set to " + board.theBoard[board.getBoardMap()[colIndex]][colIndex + (i + 1)].getVerticalNumConnected());
                }   
            } catch(Exception e) {
                below = -1;
            }
        }
    }

    /**
     * Computes the left diagonal sequence for the current position
     */
    public static void computeLeftDiagonal(Board board, int col, boolean p1Turn) {

        int left = 0;
        int right = 0;

        // Get the length for the diagonal sequence above and to the left of the current position
        try {

            // Unique checks for P1 and P2
            if(p1Turn) {
                left = board.theBoard[(board.getBoardMap()[col - 1]) - 1][col - 2].getPlayer() == 1 ? board.theBoard[(board.getBoardMap()[col - 1]) - 1][col - 2].getLeftDiagonalNumConnected() : 0;
            } else {
                left = board.theBoard[(board.getBoardMap()[col - 1]) - 1][col - 2].getPlayer() == -1 ? board.theBoard[(board.getBoardMap()[col - 1]) - 1][col - 2].getLeftDiagonalNumConnected() : 0;
            }
        } catch(Exception e) {
            left = 0;
        }

        // Get the length for the diagonal sequence below and to the right of the current position
        try {

            // Unique checks for P1 and P2
            if(p1Turn) {
                right = board.theBoard[(board.getBoardMap()[col - 1]) + 1][col].getPlayer() == 1 ? board.theBoard[(board.getBoardMap()[col - 1]) + 1][col].getLeftDiagonalNumConnected() : 0;
            } else {
                right = board.theBoard[(board.getBoardMap()[col - 1]) + 1][col].getPlayer() == -1 ? board.theBoard[(board.getBoardMap()[col - 1]) + 1][col].getLeftDiagonalNumConnected() : 0;
            }
        } catch(Exception e) {
            right = 0;
        }

        // The length of the left diagonal sequence at the current position is equal to the length of the left sequence plus the right sequence plus 1 (for the current piece)
        board.theBoard[(board.getBoardMap()[col - 1])][col - 1].setLeftDiagonalNumConnected(Math.max(left + right + 1, 1));

        propagateValueLeftDiagonally(board, board.theBoard[(board.getBoardMap()[col - 1])][col - 1].getPlayer(), 
        board.theBoard[(board.getBoardMap()[col - 1])][col - 1].getLeftDiagonalNumConnected(), 
        left,
        right,
        col - 1);

    }

    /**
     * Updates all pieces in a left diagonal sequence with the full sequence length
     */
    public static void propagateValueLeftDiagonally(Board board, int player, int value, int leftAbove, int rightBelow, int colIndex) {

        // Loop while there are still values to update either above and to the left or below and to the right of the piece placed
        for(int i = 0; i < leftAbove || i < rightBelow; i++) {

            // If there is a piece above and left, update it
            try {
                if(board.theBoard[(board.getBoardMap()[colIndex]) - (i + 1)][colIndex - (i + 1)].getPlayer() == player && i < leftAbove) {
                    board.theBoard[(board.getBoardMap()[colIndex]) - (i + 1)][colIndex - (i + 1)].setLeftDiagonalNumConnected(value);
                    // System.out.println("Left diagonal sequence length at position [" + ((board.getBoardMap()[colIndex]) - (i + 1)) + "][" + (colIndex - (i + 1)) + "] is set to " + board.theBoard[board.getBoardMap()[(board.getBoardMap()[colIndex]) - (i + 1)]][colIndex - (i + 1)].getLeftDiagonalNumConnected());
                }
            } catch(Exception e) {
                leftAbove = -1;
            }

            // If there is a piece below and right, update it 
            try { 
                if(board.theBoard[(board.getBoardMap()[colIndex]) + (i + 1)][colIndex + (i + 1)].getPlayer() == player && i < rightBelow){
                    board.theBoard[(board.getBoardMap()[colIndex]) + (i + 1)][colIndex + (i + 1)].setLeftDiagonalNumConnected(value);
                    // System.out.println("Left diagonal sequence length at position [" + ((board.getBoardMap()[colIndex]) + (i + 1)) + "][" + (colIndex + (i + 1)) + "] is set to " + board.theBoard[board.getBoardMap()[(board.getBoardMap()[colIndex]) + (i + 1)]][colIndex + (i + 1)].getLeftDiagonalNumConnected());
                }
            } catch(Exception e) {
                rightBelow = -1;
            }
        }
    }

    /**
     * Computes the right diagonal sequence for the current position
     */
    public static void computeRightDiagonal(Board board, int col, boolean p1Turn) {

        int left = 0;
        int right = 0;

        // Get the length of the sequence below and to the left of the current position
        try {

            // Unique checks for P1 and P2
            if(p1Turn) {
                left = board.theBoard[(board.getBoardMap()[col - 1]) + 1][col - 2].getPlayer() == 1 ? board.theBoard[(board.getBoardMap()[col - 1]) + 1][col - 2].getRightDiagonalNumConnected() : 0;
            } else {
                left = board.theBoard[(board.getBoardMap()[col - 1]) + 1][col - 2].getPlayer() == -1 ? board.theBoard[(board.getBoardMap()[col - 1]) + 1][col - 2].getRightDiagonalNumConnected() : 0;
            }
        } catch(Exception e) {
            left = 0;
        }

        // Get the length of the sequence above and to the right of the current position
        try {
            
            // Unique checks for P1 and P2
            if(p1Turn) {
                right = board.theBoard[(board.getBoardMap()[col - 1]) - 1][col].getPlayer() == 1 ? board.theBoard[(board.getBoardMap()[col - 1]) - 1][col].getRightDiagonalNumConnected() : 0;
            } else {
                right = board.theBoard[(board.getBoardMap()[col - 1]) - 1][col].getPlayer() == -1 ? board.theBoard[(board.getBoardMap()[col - 1]) - 1][col].getRightDiagonalNumConnected() : 0;
            }
        } catch(Exception e) {
            right = 0;
        }

        // The length of the right horizontal sequence at the current position is equal to the length of the left sequence plus the right sequence plus 1 (for the current piece)
        board.theBoard[(board.getBoardMap()[col - 1])][col - 1].setRightDiagonalNumConnected(Math.max(left + right + 1, 1));

        propagateValueRightDiagonally(board, board.theBoard[(board.getBoardMap()[col - 1])][col - 1].getPlayer(), 
        board.theBoard[(board.getBoardMap()[col - 1])][col - 1].getRightDiagonalNumConnected(), 
        left,
        right,
        col - 1);

    }

    /**
     * Updates all pieces in a right diagonal sequence with the full sequence length
     */
    public static void propagateValueRightDiagonally(Board board, int player, int value, int leftBelow, int rightAbove, int colIndex) {

        // Loop while there are still values to update either below and to the left or above and to the right of the piece placed
        for(int i = 0; i < leftBelow || i < rightAbove; i++) {

            // If there is a piece below and left, update it
            try {
                if(board.theBoard[(board.getBoardMap()[colIndex]) + (i + 1)][colIndex - (i + 1)].getPlayer() == player && i < leftBelow) {
                    board.theBoard[(board.getBoardMap()[colIndex]) + (i + 1)][colIndex - (i + 1)].setRightDiagonalNumConnected(value);
                    // System.out.println("Right diagonal sequence length at position [" + ((board.getBoardMap()[colIndex]) + (i + 1)) + "][" + (colIndex - (i + 1)) + "] is set to " + board.theBoard[board.getBoardMap()[(board.getBoardMap()[colIndex]) + (i + 1)]][colIndex - (i + 1)].getRightDiagonalNumConnected());
                }
            } catch(Exception e) {
                leftBelow = -1;
            }

            // If there is a piece above and right, update it 
            try {
                if(board.theBoard[(board.getBoardMap()[colIndex]) - (i + 1)][colIndex + (i + 1)].getPlayer() == player && i < rightAbove){
                    board.theBoard[(board.getBoardMap()[colIndex]) - (i + 1)][colIndex + (i + 1)].setRightDiagonalNumConnected(value);
                    // System.out.println("Right diagonal sequence length at position [" + ((board.getBoardMap()[colIndex]) - (i + 1)) + "][" + (colIndex + (i + 1)) + "] is set to " + board.theBoard[board.getBoardMap()[(board.getBoardMap()[colIndex]) - (i + 1)]][colIndex + (i + 1)].getRightDiagonalNumConnected());
                }
            } catch(Exception e) {
                rightAbove = -1;
            }   
        }
    }

}