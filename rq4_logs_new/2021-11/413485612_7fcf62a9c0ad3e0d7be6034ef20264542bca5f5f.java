import java.beans.PropertyChangeListener;
import java.beans.PropertyChangeSupport;

/**
 * Entity
 */
public class Board{

    private String[][] board;   // Each cell can be the name/ID of a piece
    private final PropertyChangeSupport observable;

    public Board() {
        board = new String[8][8];
        observable = new PropertyChangeSupport(this);
        reset();
    }



    public void addObserver(PropertyChangeListener observer) {
        observable.addPropertyChangeListener(observer);
    }

    public void removeObserver(PropertyChangeListener observer) {
        observable.removePropertyChangeListener(observer);
    }

    public void update() {
        observable.firePropertyChange("board", board, board);
    }




    public String[][] getBoard() {
        return board;
    }

    public void addPiece(String pieceName, int X, int Y) {
        board[X][Y] = pieceName;
        update();
    }

    public String removePiece(int X, int Y) {
        String piece = board[X][Y];
        board[X][Y] = "vacant";
        update();
        return piece;
    }

    public boolean isPositionVacant(int X, int Y) {
        return board[X][Y].equals("vacant");
    }

    public String getPiece(int X, int Y) {
        return board[X][Y];
    }

    public void reset()
    {
        // initialize white pieces
        board[7][0] = "w_rook_l";
        board[7][1] = "w_knight_l";
        board[7][2] = "w_bishop_l";
        board[7][3] = "w_queen";
        board[7][4] = "w_king";
        board[7][5] = "w_bishop_r";
        board[7][6] = "w_knight_r";
        board[7][7] = "w_rook_r";
        board[6][0] = "w_pawn_0";
        board[6][1] = "w_pawn_1";
        board[6][2] = "w_pawn_2";
        board[6][3] = "w_pawn_3";
        board[6][4] = "w_pawn_4";
        board[6][5] = "w_pawn_5";
        board[6][6] = "w_pawn_6";
        board[6][7] = "w_pawn_7";

        // initialize black pieces
        board[0][0] = "b_rook_l";
        board[0][1] = "b_knight_l";
        board[0][2] = "b_bishop_l";
        board[0][3] = "b_queen";
        board[0][4] = "b_king";
        board[0][5] = "b_bishop_r";
        board[0][6] = "b_knight_r";
        board[0][7] = "b_rook_r";
        board[1][0] = "b_pawn_0";
        board[1][1] = "b_pawn_1";
        board[1][2] = "b_pawn_2";
        board[1][3] = "b_pawn_3";
        board[1][4] = "b_pawn_4";
        board[1][5] = "b_pawn_5";
        board[1][6] = "b_pawn_6";
        board[1][7] = "b_pawn_7";

        // initialize remaining board with no pieces
        for (int i = 2; i < 6; i++) {
            for (int j = 0; j < 8; j++) {
                board[i][j] = "vacant";
            }
        }
    }


}