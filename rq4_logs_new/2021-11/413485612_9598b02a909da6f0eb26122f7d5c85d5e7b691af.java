package Board;

/**
 * Entity
 */
public class Board {

    private String[][] addPiece;   // Each cell can be the name/ID of a piece

    public Board(int column, int row) {
        addPiece = new String[column][row];
        reset();
    }

    public String[][] getBoard() {
        return addPiece;
    }

    public void addPiece(String pieceName, int X, int Y) {
        addPiece[X][Y] = pieceName;
    }

    public String removePiece(int X, int Y) {
        String piece = addPiece[X][Y];
        addPiece[X][Y] = "vacant";
        return piece;
    }

    public boolean isPositionVacant(int X, int Y) {
        return addPiece[X][Y].equals("vacant");
    }

    public String getPiece(int X, int Y) {
        return addPiece[X][Y];
    }

    public void reset()
    {
        // initialize white pieces
        addPiece[7][0] = "w_rook_l";
        addPiece[7][1] = "w_knight_l";
        addPiece[7][2] = "w_bishop_l";
        addPiece[7][3] = "w_queen";
        addPiece[7][4] = "w_king";
        addPiece[7][5] = "w_bishop_r";
        addPiece[7][6] = "w_knight_r";
        addPiece[7][7] = "w_rook_r";
        addPiece[6][0] = "w_pawn_0";
        addPiece[6][1] = "w_pawn_1";
        addPiece[6][2] = "w_pawn_2";
        addPiece[6][3] = "w_pawn_3";
        addPiece[6][4] = "w_pawn_4";
        addPiece[6][5] = "w_pawn_5";
        addPiece[6][6] = "w_pawn_6";
        addPiece[6][7] = "w_pawn_7";

        // initialize black pieces
        addPiece[0][0] = "b_rook_l";
        addPiece[0][1] = "b_knight_l";
        addPiece[0][2] = "b_bishop_l";
        addPiece[0][3] = "b_queen";
        addPiece[0][4] = "b_king";
        addPiece[0][5] = "b_bishop_r";
        addPiece[0][6] = "b_knight_r";
        addPiece[0][7] = "b_rook_r";
        addPiece[1][0] = "b_pawn_0";
        addPiece[1][1] = "b_pawn_1";
        addPiece[1][2] = "b_pawn_2";
        addPiece[1][3] = "b_pawn_3";
        addPiece[1][4] = "b_pawn_4";
        addPiece[1][5] = "b_pawn_5";
        addPiece[1][6] = "b_pawn_6";
        addPiece[1][7] = "b_pawn_7";

        // initialize remaining board with no pieces
        for (int i = 2; i < 6; i++) {
            for (int j = 0; j < 8; j++) {
                addPiece[i][j] = "vacant";
            }
        }
    }


}