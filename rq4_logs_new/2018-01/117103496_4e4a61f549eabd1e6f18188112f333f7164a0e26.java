/**
 * Interface stipulating that an AI is a class that implements ‘chooseMove’.
 *
 * STUDENTS: Nothing to change here.
 *
 * @author INSERT YOUR NAME(S) HERE
 */

public interface AI {
    /**
     * Make a move given the current state of the board.
     * 
     * @precondition There exists an open space on the board.
     */
    public Move chooseMove(Board board);
}