using System;
using System.Threading;

namespace GameCore
{
    class GameBoard
    {
        // These values will be printed to console in function DrawBoard()
        string[] positionsDrawn =
        {
        "  0 ",
        "  1 ",
        "  2 ",
        "  3 ",
        "  4 ",
        "  5 ",
        "  6 ",
        "  7 ",
        "  8 ",
        "  9 ",
        " 10 ",
        " 11 ",
        " 12 ",
        " 13 ",
        " 14 ",
        " 15 "
        };

        // Keeps track of how many pieces are availabel t0 place to see if there is a tie
        int availablePieces = 16;

        // initialize GameBoard with empty slots
        GamePiece[][] positions = new GamePiece[4][] {
            new GamePiece[] {null, null, null, null },
            new GamePiece[] {null, null, null, null },
            new GamePiece[] {null, null, null, null },
            new GamePiece[] {null, null, null, null } };

        // initialize all of the GamePieces
        GamePiece[] gamePieces = new GamePiece[16]
         {
             // black GamePieces
            new GamePiece() {color = 0, height = 0, type = 0, emblem = 0, bitValue = "0000"},
            new GamePiece() {color = 0, height = 0, type = 0, emblem = 1, bitValue = "0001"},
            new GamePiece() {color = 0, height = 0, type = 1, emblem = 0, bitValue = "0010"},
            new GamePiece() {color = 0, height = 0, type = 1, emblem = 1, bitValue = "0011"},
            new GamePiece() {color = 0, height = 1, type = 0, emblem = 0, bitValue = "0100"},
            new GamePiece() {color = 0, height = 1, type = 0, emblem = 1, bitValue = "0101"},
            new GamePiece() {color = 0, height = 1, type = 1, emblem = 0, bitValue = "0110"},
            new GamePiece() {color = 0, height = 1, type = 1, emblem = 1, bitValue = "0111"},
                                                                        
            // white GamePieces                                         
            new GamePiece() {color = 1, height = 0, type = 0, emblem = 0, bitValue = "1000"},
            new GamePiece() {color = 1, height = 0, type = 0, emblem = 1, bitValue = "1001"},
            new GamePiece() {color = 1, height = 0, type = 1, emblem = 0, bitValue = "1010"},
            new GamePiece() {color = 1, height = 0, type = 1, emblem = 1, bitValue = "1011"},
            new GamePiece() {color = 1, height = 1, type = 0, emblem = 0, bitValue = "1100"},
            new GamePiece() {color = 1, height = 1, type = 0, emblem = 1, bitValue = "1101"},
            new GamePiece() {color = 1, height = 1, type = 1, emblem = 0, bitValue = "1110"},
            new GamePiece() {color = 1, height = 1, type = 1, emblem = 1, bitValue = "1111"},
        };

        // Returns true if a piece was successfully placed
        public bool placeGamePiece()
        {
            // If position already taken returns 16; if invalid, returns 17
            int boardPosition = getPosition();

            // Since are board only goes up to 16 positions (0 - 15), 16 and 17 will never be a viable position so 
            // we can use it for an error check
            if (boardPosition > 15)
            {
                string error = (boardPosition == 16) ? "A piece was already placed there. Choose another position" :
                    "Please choose a valid position (0-15).";

                Console.Clear();
                drawBoard();
                Console.WriteLine(error);
                Console.WriteLine();
                Thread.Sleep(1000);
                return false;
            }

            // If piece is already used returns 16; if invalid, returns 17
            int gamePiece = getGamePiece();

            // Since we only have 16 pieces (0 - 15), 16 and 17 will never be a viable position so 
            // we can use it for an error check
            if (gamePiece > 15)
            {
                string error = (gamePiece == 16) ? "This GamePiece is already on the game board, choose another one." :
                     "Please choose a valid GamePiece (0-15).";

                Console.Clear();
                drawBoard();
                Console.WriteLine(error);
                Console.WriteLine();
                Thread.Sleep(1000);
                return false;
            }

            // Set row and col values
            int row = boardPosition / 4;
            int col = boardPosition % 4;

            positions[row][col] = gamePieces[gamePiece];
            updateGameBoard(boardPosition, gamePieces[gamePiece]);
            gamePieces[gamePiece] = null;
            availablePieces--;

            return true;
        }

        public int getPosition()
        {
            Console.Write("Choose a position to place a piece: ");
            int currentPosition = int.Parse(Console.ReadLine());

            // Check for out of range input
            if (currentPosition < 0 || currentPosition > 15)
                return 17;

            int row = currentPosition / 4;
            int col = currentPosition % 4;

            // Checks if there is a GamePiece on the current position
            if (positions[row][col] != null)
                return 16;

            return currentPosition;
        }

        public int getGamePiece()
        {
            Console.Write("Choose a game piece: ");
            int currentGamePiece = int.Parse(Console.ReadLine());

            // Check for out of range input
            if (currentGamePiece < 0 || currentGamePiece > 15)
                return 17;

            // Checks if the GamePiece is already used
            if (gamePieces[currentGamePiece] == null)
                return 16;

            return currentGamePiece;
        }

        // This function checks to see if there is a winner or a tie
        public bool isGameOver()
        {
            // checks the rows
            for (int i = 0; i < positions.Length; i++)
            {
                GamePiece[] result = positions[i];
                bool checkedRows = checkWinConditions(result[0], result[1], result[2], result[3]);
                if (checkedRows)
                    return true;
            }

            // checks the cols
            for (int i = 0; i < positions.Length; i++)
            {
                GamePiece[] result = new GamePiece[4];
                for (int j = 0; j < 4; j++)
                    result[j] = positions[j][i];

                bool checkedRows = checkWinConditions(result[0], result[1], result[2], result[3]);
                if (checkedRows)
                    return true;
            }

            // checks the main diagonal (left to right)
            bool diagonalResult = checkWinConditions(positions[0][0], positions[1][1], positions[2][2], positions[3][3]);
            if (diagonalResult)
                return true;

            // checks the secondary diagonal (right to left)
            bool otherDiagonalResult = checkWinConditions(positions[0][3], positions[1][2], positions[2][1], positions[3][0]);
            if (otherDiagonalResult)
                return true;

            // Checks for a tie
            if (availablePieces == 0)
                return true;

            return false;
        }

        // Checks all possible conditions of a winning move
        public bool checkWinConditions(GamePiece a, GamePiece b, GamePiece c, GamePiece d)
        {
            // checks if the other positions of the game board are empty (no GamePieces on them)          
            if (a == null || b == null || c == null || d == null)
                return false;

            // checks if there are 4 GamePieces next to each other with similiar stats
            if (a.height == b.height && a.height == c.height && a.height == d.height)
                return true;

            else if (a.color == b.color && a.color == c.color && a.color == d.color)
                return true;

            else if (a.emblem == b.emblem && a.emblem == c.emblem && a.emblem == d.emblem)
                return true;

            else if (a.type == b.type && a.type == c.type && a.type == d.type)
                return true;

            // if there arent any conditions met, that means that there isn't a winner
            return false;
        }

        // Stars a new game if the user chooses to
        public bool startNewGame()
        {
            string result = (availablePieces > 0) ? "Winner!" : "It's a tie!";

            Console.Clear();
            drawBoard();
            Console.WriteLine(result);

            Console.Write("Would you like to play again? (y/n): ");

            while (true)
            {
                string playAgain = Console.ReadLine();
                if (playAgain.ToLower() == "no" || playAgain.ToLower() == "n")
                {
                    Console.WriteLine("Goodbye!");
                    return false;
                }
                else if (playAgain.ToLower() == "yes" || playAgain.ToLower() == "y")
                {
                    Console.Clear();
                    return true;
                }
                else
                    Console.WriteLine("Please enter either \"yes\" or \"no\";");
            }
        }

        // Adds the game piece selected on the game board
        public void updateGameBoard(int position, GamePiece gamePiece)
        {
            positionsDrawn[position] = gamePiece.bitValue;
        }

        // Returns a string of game pieces not played
        public string createStringRemainingPieces()
        {
            string remainingPieces = "";

            foreach (GamePiece piece in gamePieces)
                if (piece != null)
                    remainingPieces += " " + piece.bitValue + " ";

            return remainingPieces;
        }

        // Displays game pieces remaining
        public void piecesRemaining()
        {
            string stringRemainingPieces = createStringRemainingPieces();
            Console.WriteLine("Pieces Remaining: " + stringRemainingPieces);
            Console.WriteLine();
        }

        // Draws the board to the screen
        public void drawBoard()
        {
            Console.Clear();
            Console.WriteLine("         |         |         |         |");
            Console.WriteLine("  {0}   |  {1}   |  {2}   |  {3}   |", positionsDrawn[0], positionsDrawn[1], positionsDrawn[2], positionsDrawn[3]);
            Console.WriteLine("_________|_________|_________|_________|");
            Console.WriteLine("         |         |         |         |");
            Console.WriteLine("  {0}   |  {1}   |  {2}   |  {3}   |", positionsDrawn[4], positionsDrawn[5], positionsDrawn[6], positionsDrawn[7]);
            Console.WriteLine("_________|_________|_________|_________|");
            Console.WriteLine("         |         |         |         |");
            Console.WriteLine("  {0}   |  {1}   |   {2}  |   {3}  |", positionsDrawn[8], positionsDrawn[9], positionsDrawn[10], positionsDrawn[11]);
            Console.WriteLine("_________|_________|_________|_________|");
            Console.WriteLine("         |         |         |         |");
            Console.WriteLine("  {0}   |  {1}   |   {2}  |   {3}  |", positionsDrawn[12], positionsDrawn[13], positionsDrawn[14], positionsDrawn[15]);
            Console.WriteLine("_________|_________|_________|_________|");
            Console.WriteLine();
        }
    }
}