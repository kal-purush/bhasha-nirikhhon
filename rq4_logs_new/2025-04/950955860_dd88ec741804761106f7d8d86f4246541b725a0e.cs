using UnityEngine;
using System;
using System.Collections.Generic;
using Unity.VisualScripting;

public class BulletSpawner : Unit
{
    enum BoardState
    {
        Empty = 0,
        Player = 1,
        BulletUp = 1 << 1,
        BulletLeft = 1 << 2,
        BulletDown = 1 << 3,
        BulletRight = 1 << 4,
        Bullet = BulletUp | BulletLeft | BulletDown | BulletRight,
    }

    struct Bullet
    {
        public Vector2 position;
        public int direction; // follows same convention as BoardState
    }

    class BoardNode
    {
        // Connections to nodes on the previous turn
        public List<BoardNode> prev = new List<BoardNode>();

        // Connections to nodes on the next turn
        public List<BoardNode> next = new List<BoardNode>();
    }

    [DoNotSerialize]
    public ControlInput inputTrigger;

    [DoNotSerialize]
    public ControlOutput outputTrigger;

    [DoNotSerialize]
    public ValueInput board;

    [DoNotSerialize]
    public ValueInput numBulletsToSpawn;

    [DoNotSerialize]
    public ValueOutput bulletsToSpawn;

    private List<Bullet> outputBullets;

    protected override void Definition()
    {
        inputTrigger = ControlInput(
            "SpawnBullets",
            (flow) =>
            {
                int[,] boardData = flow.GetValue<int[,]>(board);
                int numBullets = flow.GetValue<int>(numBulletsToSpawn);
                outputBullets = GetBulletSpawns(boardData, numBullets);
                return outputTrigger;
            }
        );
        outputTrigger = ControlOutput("Output");

        board = ValueInput<int[,]>("board", new int[7, 7]);
        numBulletsToSpawn = ValueInput<int>("numBulletsToSpawn", 0);
        bulletsToSpawn = ValueOutput<List<Bullet>>("bulletsToSpawn", (flow) => outputBullets);

        Succession(inputTrigger, outputTrigger);
        Assignment(inputTrigger, bulletsToSpawn);
    }

    // This function will attempt to find a set of bullet spawns
    // for the specific board state up to the number of bullets specified.
    // If there are not enough valid spawns, it will return as many as possible.
    private List<Bullet> GetBulletSpawns(int[,] board, int numBulletsToSpawn)
    {
        int boardSize = board.GetLength(0);
        // First, create a directed graph where each node is a tile and each layer is a turn.
        // A connection between two nodes is a valid move for the player that will not hit a bullet.
        // The array is indexed by (turn, row, col) or (turn, y, x)
        BoardNode[,,] boardGraph = new BoardNode[boardSize, boardSize, boardSize];
        for (int i = 0; i < boardSize; i++)
        {
            for (int j = 0; j < boardSize; j++)
            {
                for (int k = 0; k < boardSize; k++)
                {
                    boardGraph[i, j, k] = new BoardNode();
                }
            }
        }

        // Find the player
        int playerRow = -1;
        int playerCol = -1;
        for (int i = 0; i < boardSize; i++)
        {
            for (int j = 0; j < boardSize; j++)
            {
                if ((board[i, j] & (int)BoardState.Player) > 0)
                {
                    playerRow = i;
                    playerCol = j;
                    break;
                }
            }
        }
        if (playerRow == -1 || playerCol == -1)
        {
            throw new Exception("Player not found on board");
        }

        BoardNode playerNode = boardGraph[0, playerRow, playerCol];
        playerNode.prev.Add(playerNode);

        // Create the board states in advance
        int[][,] boardStates = new int[boardSize][,];
        boardStates[0] = board;
        for (int turn = 0; turn < boardSize - 1; turn++)
        {
            boardStates[turn + 1] = AdvanceBoard(boardStates[turn]);
        }

        // For each turn, the nodes that have connections to the previous turn
        // are the nodes that the player can get to. For each of those nodes,
        // check the 4 adjacent tiles on the next turn to see if they are valid moves.
        for (int turn = 0; turn < boardSize - 1; turn++)
        {
            for (int row = 0; row < boardSize; row++)
            {
                for (int col = 0; col < boardSize; col++)
                {
                    BoardNode currentNode = boardGraph[turn, row, col];
                    if (currentNode.prev.Count == 0)
                        continue; // No connections to the previous turn, skip this node

                    // Check the 4 adjacent tiles
                    for (int d = 0; d < 4; d++)
                    {
                        int nextRow = row + (d == 0 ? -1 : (d == 2 ? 1 : 0));
                        int nextCol = col + (d == 1 ? -1 : (d == 3 ? 1 : 0));
                        bool validIdx =
                            nextRow >= 0
                            && nextRow < boardSize
                            && nextCol >= 0
                            && nextCol < boardSize;
                        bool noBullet =
                            (boardStates[turn + 1][nextRow, nextCol] & (int)BoardState.Bullet) == 0;
                        if (validIdx && noBullet)
                        {
                            BoardNode nextNode = boardGraph[turn + 1, nextRow, nextCol];
                            currentNode.next.Add(nextNode);
                            nextNode.prev.Add(currentNode);
                        }
                    }
                }
            }
        }

        // Now we check each possible bullet spawn location and see if it is valid.
        // A valid spawn location is one that will not completely block player paths, that is,
        // the last layer of the graph must have at least one connection to the previous layer.
        // Bullets can only spawn on the extreme edges of the board and direct into the board
        List<Vector2> spawnLocations = new List<Vector2>();
        for (int row = 0; row < boardSize; row++)
        {
            for (int col = 0; col < boardSize; col++)
            {
                // Check if the tile is on the edge of the board
                if (row == 0 || row == boardSize - 1 || col == 0 || col == boardSize - 1)
                {
                    spawnLocations.Add(new Vector2(col, row));
                }
            }
        }

        List<List<Bullet>> validSpawnSets = new List<List<Bullet>>();

        return new List<Bullet>();
    }

    // Converts a 2D array index to a 2D position on the board
    // Multidimensional arrays are stored in row-major order
    // and (0,0) is the top left of the array,
    // while (0,0) is the center of the board.
    /* Example: size = 3
    *   (0,0) (0,1) (0,2)    (-1,1)  (0,1)  (1,1)
    *   (1,0) (1,1) (1,2) -> (-1,0)  (0,0)  (1,0)
    *   (2,0) (2,1) (2,2)    (-1,-1) (0,-1) (1,-1)
    */
    private Vector2 IndexToPosition(int row, int col, int size)
    {
        int center = size / 2;
        return new Vector2(col - center, center - row);
    }

    // Advances the board by one turn, moving the bullets
    private int[,] AdvanceBoard(int[,] board)
    {
        int boardSize = board.GetLength(0);
        int[,] nextBoard = new int[boardSize, boardSize];
        for (int row = 0; row < boardSize; row++)
        {
            for (int col = 0; col < boardSize; col++)
            {
                int bullet = board[row, col] & (int)BoardState.Bullet;
                if (bullet == 0)
                    continue;

                // Move the bullet in the direction it is facing
                // One tile can possibly have multiple bullets
                for (int d = 1; d <= 4; d++)
                {
                    int dir = bullet & (1 << d);
                    if (dir > 0)
                    {
                        int nextRow = row + (d == 1 ? -1 : (d == 3 ? 1 : 0));
                        int nextCol = col + (d == 2 ? -1 : (d == 4 ? 1 : 0));
                        bool validIdx =
                            nextRow >= 0
                            && nextRow < boardSize
                            && nextCol >= 0
                            && nextCol < boardSize;
                        if (validIdx)
                        {
                            nextBoard[nextRow, nextCol] |= dir;
                        }
                    }
                }
            }
        }
        return nextBoard;
    }

    private bool IsValidSpawn(int[,] board, int row, int col)
    {
        // Check if the tile is on the edge of the board
        if (row == 0 || row == board.GetLength(0) - 1 || col == 0 || col == board.GetLength(1) - 1)
        {
            return true;
        }
        return false;
    }
}