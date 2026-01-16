/**
 * An immutable class that represents the state of the 3×3 tic-tac-toe board.
 *
 * @author INSERT YOUR NAME(S) HERE
 */

public class Board {

    private char[][] board = new char[3][3];

    /*
     * TBD: Create additional private members if useful.
     */

    /**
     * Construct an empty board (contains all space char's).
     */
    public Board() {
        /*
         * TBD
         */
    }

    /**
     * Given the 'other' board as a starting condition, apply the given
     * 'move' to generate this board's state.
     */
    public Board(Board other, Move move) {
        /*
         * TBD
         */
    }

    /**
     * Convert to a string that shows the board's state.
     */
    public String toString() {
        /*
         * TBD
         */
    }

    /**
     * Get the entry of the board at column i, row j.  Both indices should
     * be in the range [0, 2].
     */
    public char get(int i, int j) {
        /*
         * TBD
         */
    }
    
    /**
     * @return true if there remain no empty spots on the board.
     */
    public boolean isFull() {
        /*
         * TBD
         */
    }
}