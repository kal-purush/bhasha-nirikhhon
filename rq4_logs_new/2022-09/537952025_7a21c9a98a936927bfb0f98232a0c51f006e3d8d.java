package ticTacToeCon;
/**
 * Clase que se encarga  de generar el tablero.
 */
public class Board { 
	
   // Definici�n de constantes para el tama�o del arreglo 3x3.
   public static final int ROWS = 3;
   public static final int COLS = 3;

   /** Tablero que se compone por un arreglo bidimensional cells[][] */
   Cell[][] cells;

   /** Constructor que ninicializa el tablero */
   public Board() {
      initGame();
   }

   /** Inicializaci�n del tablero una sola vez */
   public void initGame() {
      cells = new Cell[ROWS][COLS];  // Arreglo bidimensional.
      for (int row = 0; row < ROWS; ++row) {
         for (int col = 0; col < COLS; ++col) {
            // Coloca el elemento dentro del array
            cells[row][col] = new Cell(row, col);
         }
      }
   }

   /** Resetea el tablero para un juego nuevo */
   public void newGame() {
      for (int row = 0; row < ROWS; ++row) {
         for (int col = 0; col < COLS; ++col) {
            cells[row][col].newGame();  // inicia las celdas del arreglo.
         }
      }
   }

   /**
    *  El jugador actual hace un movimiento con (player, selectedRow, selectedCol).
    *  Despues se verifica si el jugador actual gana y cambia el estado.
    *  Verifica si hay posibilidad de tirar y regresa el estado PLAYING.
    *  En caso de que nadie gane y que no halla espacios vacios regresa el estado DRAW.
    */
   public State stepGame(Seed player, int selectedRow, int selectedCol) {
      // Update game board
      cells[selectedRow][selectedCol].content = player;

      // Retorna el nuevo estado
      if (cells[selectedRow][0].content == player  // 3 linea.
                && cells[selectedRow][1].content == player
                && cells[selectedRow][2].content == player
             || cells[0][selectedCol].content == player // 3 columna.
                && cells[1][selectedCol].content == player
                && cells[2][selectedCol].content == player
             || selectedRow == selectedCol         // 3 en diagonal.
                && cells[0][0].content == player
                && cells[1][1].content == player
                && cells[2][2].content == player
             || selectedRow + selectedCol == 2     // 3 en diagonal inversa.
                && cells[0][2].content == player
                && cells[1][1].content == player
                && cells[2][0].content == player) {
         return (player == Seed.CROSS) ? State.CROSS_WON : State.NOUGHT_WON;
      } else {
         // Si nadie gana. Verifica si hay espacios en blanco y regresa PLAYING, de lo contrario regresa DRAW.
         for (int row = 0; row < ROWS; ++row) {
            for (int col = 0; col < COLS; ++col) {
               if (cells[row][col].content == Seed.NO_SEED) {
                  return State.PLAYING; // Aun quedan celdas vacias.
               }
            }
         }
         return State.DRAW; // No hay celdas vacias. Es empate.
      }
   }

   /** Se dibuja el tablero */
   public void paint() {
      for (int row = 0; row < ROWS; ++row) {
         for (int col = 0; col < COLS; ++col) {
            System.out.print(" ");
            cells[row][col].paint();   // Cada celda se dibuja.
            System.out.print(" ");
            if (col < COLS - 1) System.out.print("|");  // Separador de columnas.
         }
         System.out.println();
         if (row < ROWS - 1) {
            System.out.println("-----------");  // Separador de Lineas.
         }
      }
      System.out.println();
   }
}