import java.awt.*;
import java.awt.event.*;
import javax.swing.*;
import java.util.ArrayList;
import javax.xml.parsers.*;
import org.w3c.dom.*;
import java.io.*;


/**
 * This panel lets two users play checkers against each other.
 * Red always starts the game.  If a player can jump an opponent's
 * piece, then the player must jump.  When a player can make no more
 * moves, the game ends.
 *
 * The class has a main() routine that lets it be run as a stand-alone
 * application.
 */

public class chp11_6 extends JPanel {
  /**
   * Main routine makes it possible to run Checkers as a stand-alone
   * application.  Opens a window showing a Checkers panel; the program
   * ends when the user closes the window.
  */
  public static void main(String[] args) {
    JFrame window = new JFrame("chp11_6");
    chp11_6 content = new chp11_6();
    window.setContentPane(content);
    window.pack();
    Dimension screenSize = Toolkit.getDefaultToolkit().getScreenSize();
    window.setLocation((screenSize.width - window.getWidth()) / 2,
      (screenSize.height - window.getHeight()) / 2);
    window.setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE);
    window.setResizable(false);
    window.setVisible(true);
  } // end main

  //---------------------------------------------------------------------

  private JButton newButton;
  private JButton resignButton;
  private JButton saveButton;
  private JButton loadButton;

  private JLabel message;

  /**
   * The constructor creates the Board (which in turn creates and manages
   * the buttons and message label), adds all the components, and sets
   * the bounds of the components.  A null layout is used.  (This is
   * the only thing that is done in the main Checkers class.)
  */
  public chp11_6() {
    setLayout(null);
    setPreferredSize(new Dimension(700, 500));
    setBackground(new Color(0,150,0));

    /* Create the components and add them to the panel. */
    Board board = new Board();

    add(board);
    add(newButton);
    add(resignButton);
    add(saveButton);
    add(loadButton);
    add(message);

    /* Set the position and size of each component by calling
    its setBounds() method. */
    board.setBounds(40,40,324,324);
    newButton.setBounds(540,60,120,60);
    resignButton.setBounds(540,130,120,60);
    saveButton.setBounds(540,200,120,60);
    loadButton.setBounds(540,270,120,60);
    message.setBounds(0,400,700,60);
  } // end constructor

  // --------------------  Nested Classes -------------------------------

  /**
   * A Move object represents a move in the game of Checkers.
   * It holds the row and column of the piece that is to be moved
   * and the row and column of the square to which it is to be moved.
   * (This class makes no guarantee that the move is legal.)
   */
  private static class Move {
    int fromRow, fromCol;
    int toRow, toCol;

    Move(int r1, int c1, int r2, int c2) {
      fromRow = r1;
      fromCol = c1;
      toRow = r2;
      toCol = c2;
    }
    boolean isJump() {
      /**
       * Test whether this move is a jump.  It is assumed that
       * the move is legal.  In a jump, the piece moves two
       * rows.  (In a regular move, it only moves one row.)
       */
      return (fromRow - toRow == 2 || fromRow - toRow == -2);
    }
  } // end Move

  /**
   * This panel displays a 320-by-320 checkerboard pattern with
   * a 2-pixel black border.  It is assumed that the size of the
   * panel is set to exactly 328-by-328 pixels.  This class does
   * the work of letting the users play checkers, and it displays
   * the checkerboard.
   */
  private class Board extends JPanel implements ActionListener, MouseListener {
    Data board; // The data for the checkers board is kept here.
                 // This board is also responsible for generating
                 // lists of legal moves
    boolean gameInProgress;

    /* The next three variables are valid only when the game is in progress. */
    int currentPlayer;
    int selectedRow, selectedCol;
    Move[] legalMoves;

    /**
     * Constructor.  Create the buttons and label.  Listens for mouse
     * clicks and for clicks on the buttons.  Create the board and
     * start the first game.
     */
    Board() {
      setBackground(Color.BLACK);
      addMouseListener(this);
      newButton = new JButton("New Game");
      newButton.addActionListener(this);
      resignButton = new JButton("Resign");
      resignButton.addActionListener(this);
      saveButton = new JButton("Save Game");
      saveButton.addActionListener(this);
      loadButton = new JButton("Load Game");
      loadButton.addActionListener(this);
      message = new JLabel("",JLabel.CENTER);
      message.setFont(new Font("Serif",Font.BOLD,28));
      message.setForeground(Color.GREEN);
      board = new Data();
      doNewGame();
    }

    /**
     * Respond to user's click on one of the two buttons.
     */
    public void actionPerformed(ActionEvent e) {
      Object src = e.getSource();
      if (src == newButton)
        doNewGame();
      else if (src == resignButton)
        doResign();
      else if (src == saveButton)
        doSave();
      else if (src == loadButton)
        doLoad();
    }

    /**
     * Start a new game
     */
    void doNewGame() {
      if (gameInProgress) {
        message.setText("You must finish the current game first!");
        return;
      }
      board.setUpGame();
      currentPlayer = Data.RED;
      legalMoves = board.getLegalMoves(Data.RED);
      selectedRow = -1;
      message.setText("Red, make your move.");
      gameInProgress = true;
      newButton.setEnabled(false);
      resignButton.setEnabled(true);
      saveButton.setEnabled(true);
      loadButton.setEnabled(true);
      repaint();
    } // end doNewGame()

    /**
     * Current player resigns.  Game ends.  Opponent wins.
     */
    void doResign() {
      if (!gameInProgress) {
        message.setText("There is no game in progress!");
        return;
      }
      if (currentPlayer == Data.RED)
        gameOver("Red resigns, Black wins.");
      else
        gameOver("Black resigns, Red wins.");
     } // end doResign()

     void doSave() {
       File saveDir = new File(System.getProperty("user.dir"), "/saves");
       File saveFile = new File(saveDir, "save_file.xml");

       if (!saveDir.exists()) {
         try {
           saveDir.mkdir();
         } catch (SecurityException e) {
           message.setText("Could not save game: " + e);
           return;
         }
       }

       try (PrintWriter out = new PrintWriter(saveFile)) {
         out.println("<?xml version=\"1.0\"?>");
         out.println("<checkers_save_file>");
         out.println("  <player value='" + currentPlayer + "'/>");
         out.println("  <board>");
         for (int row = 0; row < 8; row++) {
           for (int col = 0; col < 8; col++) {
             out.print("    <square row='" + row + "' ");
             out.print("col='" + col + "' ");
             out.println("data='" + board.board[row][col] + "'/>");
           }
         }
         out.println("  </board>");
         out.println("</checkers_save_file>");
         out.flush();
       } catch (IOException e) {
         // custom message for this case?
         System.out.println("An error occurred while saving: " + e);
       } catch (Exception e) {
         System.out.println("An error occurred while saving: " + e);
       }
       loadButton.setEnabled(true);
     } // end doSave()

     void doLoad() {
       File loadDir = new File(System.getProperty("user.dir"), "/saves");
       File loadFile = new File(loadDir, "save_file.xml");
       Data newBoard = new Data();

       if (!loadFile.exists()) {
         message.setText("No save file found...");
         return;
       }

       message.setText("Loading saved game from " + loadFile.getName());
       try {
         DocumentBuilder reader = DocumentBuilderFactory.newInstance().newDocumentBuilder();
         Document xmldoc = reader.parse(loadFile);
         Element root = xmldoc.getDocumentElement();
         if (!root.getTagName().equals("checkers_save_file")) {
           throw new Exception();
         }
         NodeList nodes = root.getChildNodes();
         for (int i = 0; i < nodes.getLength(); i++) {
           if (nodes.item(i) instanceof Element) {
             Element data = (Element)nodes.item(i);
             if (data.getTagName().equals("player")) {
               currentPlayer = Integer.parseInt(data.getAttribute("value"));
             } else if (data.getTagName().equals("board")) {
               NodeList children = data.getChildNodes();
               for (int j = 0; j < children.getLength(); j++) {
                 if (children.item(j) instanceof Element) {
                   Element square = (Element)children.item(j);
                   if (square.getTagName().equals("square")) {
                     int row = Integer.parseInt(square.getAttribute("row"));
                     int col = Integer.parseInt(square.getAttribute("col"));
                     int value = Integer.parseInt(square.getAttribute("data"));
                     newBoard.board[row][col] = value;
                   }
                 }
               }
             } else {
               throw new Exception();
             }
           }
         }
       } catch (Exception e) {
         message.setText("Error loading game: " + e);
       }

       String player;
       if (currentPlayer == board.BLACK) {
         player = "Black";
       } else {
         player = "Red";
       }
       board = newBoard;
       legalMoves = board.getLegalMoves(currentPlayer);
       selectedRow = -1;
       gameInProgress = true;
       newButton.setEnabled(false);
       resignButton.setEnabled(true);
       saveButton.setEnabled(true);
       loadButton.setEnabled(false);
       message.setText("Game loaded, it is " + player + "'s turn");
       repaint();
     } // end doLoad()

    /**
     * The game ends.  The parameter, str, is displayed as a message
     * to the user.  The states of the buttons are adjusted so players
     * can start a new game.  This method is called when the game
     * ends at any point in this class.
     */
    void gameOver(String str) {
      message.setText(str);
      newButton.setEnabled(true);
      resignButton.setEnabled(false);
      saveButton.setEnabled(false);
      loadButton.setEnabled(true);
      gameInProgress = false;
    } // end gameOver()

    /**
     * This is called by mousePressed() when a player clicks on the
     * square in the specified row and col.  It has already been checked
     * that a game is, in fact, in progress.
     */
    void doClickSquare(int row, int col) {
      /* If the player clicked on one of the pieces that the player
         can move, mark this row and col as selected and return.  (This
         might change a previous selection.)  Reset the message, in
         case it was previously displaying an error message. */
      for (int i = 0; i < legalMoves.length; i++) {
        if (legalMoves[i].fromRow == row && legalMoves[i].fromCol == col) {
          selectedRow = row;
          selectedCol = col;
          if (currentPlayer == Data.RED)
            message.setText("Red, make your move.");
          else
            message.setText("Black, make your move.");
          repaint();
          return;
         }
       }

       /* If no piece has been selected to be moved, the user must first
          select a piece.  Show an error message and return. */
       if (selectedRow < 0) {
         message.setText("Click the piece you want to move.");
         return;
       }

        /* If the user clicked on a square where the selected piece can be
           legally moved, then make the move and return. */
        for (int i = 0; i < legalMoves.length; i++) {
          if (legalMoves[i].fromRow == selectedRow && legalMoves[i].fromCol == selectedCol
            && legalMoves[i].toRow == row && legalMoves[i].toCol == col) {
            doMakeMove(legalMoves[i]);
            return;
          }
        }

        /* If we get to this point, there is a piece selected, and the square where
           the user just clicked is not one where that piece can be legally moved.
           Show an error message. */
        message.setText("Click the square you want to move to.");
    } // end doClickSquare()

    /**
     * This is called when the current player has chosen the specified
     * move.  Make the move, and then either end or continue the game
     * appropriately.
     */
    void doMakeMove(Move move) {
      board.makeMove(move);

      /* If the move was a jump, it's possible that the player has another
         jump.  Check for legal jumps starting from the square that the player
         just moved to.  If there are any, the player must jump.  The same
         player continues moving.
      */
      if (move.isJump()) {
        legalMoves = board.getLegalJumpsFrom(currentPlayer, move.toRow, move.toCol);
        if (legalMoves != null) {
          if (currentPlayer == Data.RED)
            message.setText("Red, you must continue jumping.");
          else
            message.setText("Black, you must continue jumping");
          selectedRow = move.toRow;
          selectedCol = move.toCol;
          repaint();
          return;
        }
      }

      /* The current player's turn is ended, so change to the other player.
         Get that player's legal moves.  If the player has no legal moves,
         then the game ends. */
      if (currentPlayer == Data.RED) {
        currentPlayer = Data.BLACK;
        legalMoves = board.getLegalMoves(Data.BLACK);
        if (legalMoves == null) {
          gameOver("Black has no moves, Red wins.");
        } else if (legalMoves[0].isJump()) {
          message.setText("Black, you must jump.");
        } else {
          message.setText("Black, make your move.");
        }
      } else {
        currentPlayer = Data.RED;
        legalMoves = board.getLegalMoves(Data.RED);
        if (legalMoves == null) {
          gameOver("Red has no moves, Black wins.");
        } else if (legalMoves[0].isJump()) {
          message.setText("Red, you must jump.");
        } else {
          message.setText("Red, make your move.");
        }
      }

      /* Set selectedRow = -1 to record that the player has not yet selected
         a piece to move. */
      selectedRow = -1;

      /* As a courtesy to the user, if all legal moves use the same piece, then
         select that piece automatically so the user won't have to click on it
         to select it. */
      if (legalMoves != null) {
        boolean sameStartSquare = true;
        for (int i = 0; i < legalMoves.length; i++) {
          if (legalMoves[i].fromRow != legalMoves[0].fromRow
            || legalMoves[i].fromCol != legalMoves[0].fromCol) {
            sameStartSquare = false;
            break;
          }
        }
        if (sameStartSquare) {
          selectedRow = legalMoves[0].fromRow;
          selectedCol = legalMoves[0].fromCol;
        }
      }

      repaint();
    } // end doMakeMove()

    /**
     * Draw a checkerboard pattern in gray and lightGray.  Draw the
     * checkers.  If a game is in progress, highlight the legal moves.
     */
    public void paintComponent(Graphics g) {
      Graphics2D g2 = (Graphics2D)g;
      g2.setRenderingHint(RenderingHints.KEY_ANTIALIASING,
        RenderingHints.VALUE_ANTIALIAS_ON);

      g.setFont(new Font("Times New Roman",Font.PLAIN,24));

      /* Draw a two-pixel black border around the edges of the canvas. */
      g.setColor(Color.BLACK);
      g.drawRect(0,0,getSize().width-1,getSize().height-1);
      g.drawRect(1,1,getSize().width-3,getSize().height-3);

      /* Draw the squares of the checkerboard and the checkers. */
      for (int row = 0; row < 8; row++) {
        for (int col = 0; col < 8; col++) {
          if (row % 2 == col % 2) {
            g.setColor(Color.LIGHT_GRAY);
          } else {
            g.setColor(Color.GRAY);
          }
          g.fillRect(2 + col*40, 2 + row*40, 40, 40);
          switch (board.pieceAt(row,col)) {
            case Data.RED:
              g.setColor(Color.RED);
              g.fillOval(7 + col*40, 7 + row*40, 30, 30);
              break;
            case Data.BLACK:
              g.setColor(Color.BLACK);
              g.fillOval(7 + col*40, 7 + row*40, 30, 30);
              break;
            case Data.RED_KING:
              g.setColor(Color.RED);
              g.fillOval(7 + col*40, 7 + row*40, 30, 30);
              g.setColor(Color.WHITE);
              g.drawString("K", 14 + col*40, 30 + row*40);
              break;
            case Data.BLACK_KING:
              g.setColor(Color.BLACK);
              g.fillOval(7 + col*40, 7 + row*40, 30, 30);
              g.setColor(Color.WHITE);
              g.drawString("K", 14 + col*40, 30 + row*40);
              break;
          }
        }
      }

      /* If a game is in progress, highlight the legal moves.   Note that legalMoves
         is never null while a game is in progress. */
      if (gameInProgress) {
        g.setColor(Color.CYAN);
        for (int i = 0; i < legalMoves.length; i++) {
          g.drawRect(2 + legalMoves[i].fromCol*40,2 + legalMoves[i].fromRow*40,39,39);
          g.drawRect(3 + legalMoves[i].fromCol*40,3 + legalMoves[i].fromRow*40,37,37);
        }
        /* If a piece is selected for moving (i.e. if selectedRow >= 0), then
           draw a 2-pixel white border around that piece and draw green borders
           around each square that that piece can be moved to. */
        if (selectedRow >= 0) {
          g.setColor(Color.WHITE);
          g.drawRect(2 + selectedCol*40,2 + selectedRow*40,39,39);
          g.drawRect(3 + selectedCol*40,3 + selectedRow*40,37,37);
          g.setColor(Color.GREEN);
          for (int i = 0; i < legalMoves.length; i++) {
            if (legalMoves[i].fromCol == selectedCol && legalMoves[i].fromRow == selectedRow) {
              g.drawRect(2 + legalMoves[i].toCol*40,2 + legalMoves[i].toRow*40,39,39);
              g.drawRect(3 + legalMoves[i].toCol*40,3 + legalMoves[i].toRow*40,37,37);
            }
          }
        }
      }
    } // end paintComponent()

    /**
     * Respond to a user click on the board.  If no game is in progress, show
     * an error message.  Otherwise, find the row and column that the user
     * clicked and call doClickSquare() to handle it.
     */
    public void mousePressed(MouseEvent e) {
      if (!gameInProgress) {
        message.setText("Click 'New game' to start a new game.");
      } else {
        int col = (e.getX() - 2) / 40;
        int row = (e.getY() - 2) / 40;
        if (col >= 0 && col < 8 && row >= 0 && row < 8) {
          doClickSquare(row, col);
        }
      }
    } // end mousePressed()
    public void mouseReleased(MouseEvent e) { }
    public void mouseClicked(MouseEvent e) { }
    public void mouseEntered(MouseEvent e) { }
    public void mouseExited(MouseEvent e) { }
  } // end Board

  /**
   * An object of this class holds data about a game of checkers.
   * It knows what kind of piece is on each square of the checkerboard.
   * Note that RED moves "up" the board (i.e. row number decreases)
   * while BLACK moves "down" the board (i.e. row number increases).
   * Methods are provided to return lists of available legal moves.
   */
  private static class Data {
    /* The following constants represent the possible contents of a square
       on the board.  The constants RED and BLACK also represent players
       in the game. */
    static final int EMPTY = 0, RED = 1, RED_KING = 2, BLACK = 3, BLACK_KING = 4;
    int[][] board;

    /**
     * Constructor.  Create the board and set it up for a new game.
     */
    Data() {
      board = new int[8][8];
      setUpGame();
    }

    /**
     * Set up the board with checkers in position for the beginning
     * of a game.  Note that checkers can only be found in squares
     * that satisfy  row % 2 == col % 2.  At the start of the game,
     * all such squares in the first three rows contain black squares
     * and all such squares in the last three rows contain red squares.
     */
    void setUpGame() {
      for (int row = 0; row < 8; row++) {
        for (int col = 0; col < 8; col++) {
          if (row % 2 == col % 2) {
            if (row < 3)
              board[row][col] = BLACK;
            else if (row > 4)
              board[row][col] = RED;
            else
              board[row][col] = EMPTY;
          } else
            board[row][col] = EMPTY;
        }
      }
    } // end setUpGame()

    /**
     * Return the contents of the square in the specified row and column.
     */
    int pieceAt(int row, int col) {
      return board[row][col];
    } // end pieceAt()

    /**
     * Make the specified move.  It is assumed that move
     * is non-null and that the move it represents is legal.
     */
    void makeMove(Move move) {
      makeMove(move.fromRow, move.fromCol, move.toRow, move.toCol);
    } // end makeMove()

    /**
     * Make the move from (fromRow,fromCol) to (toRow,toCol).  It is
     * assumed that this move is legal.  If the move is a jump, the
     * jumped piece is removed from the board.  If a piece moves to
     * the last row on the opponent's side of the board, the
     * piece becomes a king.
     */
    void makeMove(int fromRow, int fromCol, int toRow, int toCol) {
      board[toRow][toCol] = board[fromRow][fromCol];
      board[fromRow][fromCol] = EMPTY;
      if (fromRow - toRow == 2 || fromRow - toRow == -2) {
        // The move is a jump
        int jumpRow = (fromRow + toRow) / 2;
        int jumpCol = (fromCol + toCol) / 2;
        board[jumpRow][jumpCol] = EMPTY;
      }
      if (toRow == 0 && board[toRow][toCol] == RED)
        board[toRow][toCol] = RED_KING;
      if (toRow == 7 && board[toRow][toCol] == BLACK)
        board[toRow][toCol] = BLACK_KING;
    } // end makeMove()

    /**
     * Return an array containing all the legal CheckersMoves
     * for the specified player on the current board.  If the player
     * has no legal moves, null is returned.  The value of player
     * should be one of the constants RED or BLACK; if not, null
     * is returned.  If the returned value is non-null, it consists
     * entirely of jump moves or entirely of regular moves, since
     * if the player can jump, only jumps are legal moves.
     */
    Move[] getLegalMoves(int player) {
      if (player != RED && player != BLACK)
        return null;

      int playerKing;
      if (player == RED)
        playerKing = RED_KING;
      else
        playerKing = BLACK_KING;

      ArrayList<Move> moves = new ArrayList<Move>();

      /* First, check for any possible jumps.  Look at each square on the board.
         If that square contains one of the player's pieces, look at a possible
         jump in each of the four directions from that square.  If there is
         a legal jump in that direction, put it in the moves ArrayList.
      */
      for (int row = 0; row < 8; row++) {
        for (int col = 0; col < 8; col++) {
          if (board[row][col] == player || board[row][col] == playerKing) {
            if (canJump(player,row,col,row+1,col+1,row+2,col+2))
              moves.add(new Move(row,col,row+2,col+2));
            if (canJump(player,row,col,row-1,col+1,row-2,col+2))
              moves.add(new Move(row,col,row-2,col+2));
            if (canJump(player,row,col,row+1,col-1,row+2,col-2))
              moves.add(new Move(row,col,row+2,col-2));
            if (canJump(player,row,col,row-1,col-1,row-2,col-2))
              moves.add(new Move(row,col,row-2,col-2));
          }
        }
      }

      /* If any jump moves were found, then the user must jump, so we don't
         add any regular moves.  However, if no jumps were found, check for
         any legal regular moves.  Look at each square on the board.
         If that square contains one of the player's pieces, look at a possible
         move in each of the four directions from that square.  If there is
         a legal move in that direction, put it in the moves ArrayList.
      */
      if (moves.size() == 0) {
        for (int row = 0; row < 8; row++) {
          for (int col = 0; col < 8; col++) {
            if (board[row][col] == player || board[row][col] == playerKing) {
              if (canMove(player,row,col,row+1,col+1))
                moves.add(new Move(row,col,row+1,col+1));
              if (canMove(player,row,col,row-1,col+1))
                moves.add(new Move(row,col,row-1,col+1));
              if (canMove(player,row,col,row+1,col-1))
                moves.add(new Move(row,col,row+1,col-1));
              if (canMove(player,row,col,row-1,col-1))
                moves.add(new Move(row,col,row-1,col-1));
            }
          }
        }
      }

      /* If no legal moves have been found, return null.  Otherwise, create
         an array just big enough to hold all the legal moves, copy the
         legal moves from the ArrayList into the array, and return the array. */
      if (moves.size() == 0)
        return null;
      else {
        Move[] moveArray = new Move[moves.size()];
        for (int i = 0; i < moves.size(); i++)
          moveArray[i] = moves.get(i);
        return moveArray;
      }
    } // end getLegalMoves()

    /**
     * Return a list of the legal jumps that the specified player can
     * make starting from the specified row and column.  If no such
     * jumps are possible, null is returned.  The logic is similar
     * to the logic of the getLegalMoves() method.
     */
    Move[] getLegalJumpsFrom(int player, int row, int col) {
      if (player != RED && player != BLACK)
        return null;

      int playerKing;
      if (player == RED)
        playerKing = RED_KING;
      else
        playerKing = BLACK_KING;

      ArrayList<Move> moves = new ArrayList<Move>();

      if (board[row][col] == player || board[row][col] == playerKing) {
        if (canJump(player,row,col,row+1,col+1,row+2,col+2))
          moves.add(new Move(row,col,row+2,col+2));
        if (canJump(player,row,col,row-1,col+1,row-2,col+2))
          moves.add(new Move(row,col,row-2,col+2));
        if (canJump(player,row,col,row+1,col-1,row+2,col-2))
          moves.add(new Move(row,col,row+2,col-2));
        if (canJump(player,row,col,row-1,col-1,row-2,col-2))
          moves.add(new Move(row,col,row-2,col-2));
      }
      if (moves.size() == 0)
        return null;
      else {
        Move[] moveArray = new Move[moves.size()];
        for (int i = 0; i < moves.size(); i++)
          moveArray[i] = moves.get(i);
        return moveArray;
      }
    } // end getLegalJumpsFrom()

    /**
     * This is called by the two previous methods to check whether the
     * player can legally jump from (r1,c1) to (r3,c3).  It is assumed
     * that the player has a piece at (r1,c1), that (r3,c3) is a position
     * that is 2 rows and 2 columns distant from (r1,c1) and that
     * (r2,c2) is the square between (r1,c1) and (r3,c3).
     */
    private boolean canJump(int player, int r1, int c1, int r2, int c2, int r3, int c3) {
      if (r3 < 0 || r3 >= 8 || c3 < 0 || c3 >= 8)
        return false;

      if (board[r3][c3] != EMPTY)
        return false;

      if (player == RED) {
        if (board[r1][c1] == RED && r3 > r1)
          return false;
        if (board[r2][c2] != BLACK && board[r2][c2] != BLACK_KING)
          return false;
        return true;
      } else {
        if (board[r1][c1] == BLACK && r3 < r1)
          return false;
        if (board[r2][c2] != RED && board[r2][c2] != RED_KING)
          return false;
        return true;
      }
    } // end canJump()

    /**
     * This is called by the getLegalMoves() method to determine whether
     * the player can legally move from (r1,c1) to (r2,c2).  It is
     * assumed that (r1,r2) contains one of the player's pieces and
     * that (r2,c2) is a neighboring square.
     */
    private boolean canMove(int player, int r1, int c1, int r2, int c2) {
      if (r2 < 0 || r2 >= 8 || c2 < 0 || c2 >= 8)
        return false;

      if (board[r2][c2] != EMPTY)
        return false;

      if (player == RED) {
        if (board[r1][c1] == RED && r2 > r1)
          return false;
        return true;
      } else {
        if (board[r1][c1] == BLACK && r2 < r1)
          return false;
        return true;
      }
    } // end canMove()
  } // end Data
} // end class chp11_6