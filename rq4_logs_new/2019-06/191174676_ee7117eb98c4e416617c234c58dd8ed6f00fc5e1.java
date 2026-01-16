/**
  Represent a square chess board for a queens puzzle
  of a particular size.
 */

public class BoardForQueensPuzzle {
    private int lastRankFilled;  /* the highest-numbered rank
      that holds a queen. */
    private int[] filesWithQueens;  /* indexed by rank, giving
      the file number that a queen occupies in that rank.
      Interacts with lastRankFilled: entries in filesWithQueens
      are meaningful only for ranks <= lastRankFilled. Entries
      at higher indexes hold bit patterns that can be interpreted
      as integers, as always. But those entries are meaningless.
      This common thinking was used for the filledElements field
      in List_inArraySlots exactly the same way.
      */


    /**
      A copy constructor

      @return a reference to a copy of @old board, with the copy
              having a reference to its own, separate copy of
              filesWithQueens.
     */
    public BoardForQueensPuzzle( BoardForQueensPuzzle old) {
        filesWithQueens = new int[old.filesWithQueens.length];
		for (int i = 0; i < old.filesWithQueens.length; i++){
			filesWithQueens[i] = old.filesWithQueens[i];
		} //Or use the object clone method on the filesWithQueens. 
		lastRankFilled = old.lastRankFilled;
    }



    // ----- skeletal code below here needs no modification -----
    //       since it was tested for previous homework

    /**
      Construct an empty instance of the specified size
     */
    public BoardForQueensPuzzle( int ranks) {
        filesWithQueens = new int[ ranks];
        lastRankFilled = -1;  // rank 0 is the next to fill
    }


    /**
      @return the size of the board
     */
    public int ranks() {
        return filesWithQueens.length;
    }


    /**
      @return the boolean value of the statement
              "The last-added queen introduced a conflict."

      @precondition: Before the latest queen was added,
                     no queen attacked another.
     */
    public boolean lastIsNg() {
        // empty boards are ok trivially
        if( lastRankFilled < 0) return false;

        int fileOfLast = filesWithQueens[ lastRankFilled];
        for( int previouslyFilledRank = 0
           ; previouslyFilledRank <  lastRankFilled
           ; previouslyFilledRank++
           ) {
            int fileOfPrev = filesWithQueens[ previouslyFilledRank];
            // There are 2 ways to conflict:
            if(
                // on the same file
                fileOfLast == fileOfPrev

                ||

                /* using integer arithmetic: on same diagonal
                   if difference in ranks
                   == difference in files or its negative
                 */
                   lastRankFilled - previouslyFilledRank  // always +
                == Math.abs( fileOfLast - fileOfPrev)
                ) return true;
           }
           return false;
    }


    /**
      @return the boolean value of the statement...
                "The board is fully and legally populated."

      precondition: All ranks except possibly the last-filled
        are populated legally. (That is, considering only the
        ranks before the last-filled rank, no queen attacks
        another.

        This method checks the last-filled rank.
     */
    public boolean accept() {
        return    lastRankFilled == filesWithQueens.length - 1
               && ! lastIsNg();
    }


    /**
      Populate the next rank with a queen in position @file
     */
    public void populate( int file) {
        filesWithQueens[ ++lastRankFilled] = file;
        /* Recall that ++lastRankFilled increases the value
           in lastRankFilled, and uses the new values as the
           value of the expression. That is, the new, higher
           value is used as the index into filesWithQueens. */
    }


    /**
      Un-do a populate(), to have no queen in the
      most-recently-populated rank.
      @precondition: Some rank(s) have been populated.
     */
    public void depopulate() {
        lastRankFilled--;
    }


    private static final String INDENT = "         ";
    /**
      @return a string representation of this board
     */
    public String toString() {
        int size = filesWithQueens.length; // just for shorthand

        /* Make a header containing file numbers like...
               3 ranks, populated through rank 1
               files
               0  1  2  3 ...
               __________ ...
         */

        // sizes and labels that precede the ranks
        String pic = INDENT
          + size + " ranks, "
          + ( // ?: conditional operator
                lastRankFilled < 0
              ? "unpopulated"
              : "populated through rank " + lastRankFilled
            )
          + ", in board with ID "
          + String.format( "%x", this.hashCode())
          + " with filesWithQueens "
          + String.format( "%x", filesWithQueens.hashCode())
          + System.lineSeparator();

        // short-circuit the picture for a zero-sized board
        if( size == 0) return pic;

        pic += INDENT + "files" + System.lineSeparator()
            +  INDENT;
        String underscores = "";

        // file numbers, each in a 3-column field
        for( int file = 0; file < size; file++) {
            pic += String.format("%-3d", file);
            underscores += "___"; // there's gotta be a better way
        }

        // underline the header
        pic+=   System.lineSeparator()
              + INDENT + underscores + System.lineSeparator();

        // picture each rank
        for( int rank = 0; rank < size; rank++){
            pic += // right-justified rank number:
                   String.format("rank %2d|", rank);
            for( int file = 0; file < size; file++)
                if(    rank > lastRankFilled      // no queen in this rank yet
                    || filesWithQueens[ rank] != file) // no queen in this file
                     pic += " _ ";
                else pic += " Q ";
            pic += System.lineSeparator();
        }
        return pic;
    }
}