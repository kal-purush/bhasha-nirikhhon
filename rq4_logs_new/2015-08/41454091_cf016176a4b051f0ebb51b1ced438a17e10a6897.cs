using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;

namespace Sokoban.Console
{
    internal class Program
    {
        public static bool RequiresRedraw = true;
        public static int YSize = 5;
        public static int XSize = 6;
        public static char[,] Map =
        {
            {'#', '#', '#', '#', '#', '#'},
            {'#', ' ', ' ', ' ', ' ', '#'},
            {'#', ' ', '.', 'o', ' ', '#'},
            {'#', ' ', ' ', ' ', '@', '#'},
            {'#', '#', '#', '#', '#', '#'}
        };

        private static bool CheckForWin()
        {
            for (var i = 0; i < YSize; i++)
            {
                for (var j = 0; j < XSize; j++)
                {
                    if (Map[i, j] == 'o')
                        return false;
                }
            }

            return true;
        }

        private static void RenderWorld()
        {
            if (RequiresRedraw == false)
            {
                return;
            }

            System.Console.Clear();
            for (var i = 0; i < YSize; i++)
            {
                for (var j = 0; j < XSize; j++)
                {
                    switch (Map[i, j])
                    {
                        case '#':
                            System.Console.Write('▓');
                            break;
                        case ' ':
                            System.Console.Write(' ');
                            break;
                        case '@':
                            System.Console.Write('☺');
                            break;
                        case 'o':
                            System.Console.Write('o');
                            break;
                        case '.':
                            System.Console.Write('.');
                            break;
                        case '+':
                            System.Console.Write('☻');
                            break;
                        case '*':
                            System.Console.Write('Θ');
                            break;
                    }

                }
                System.Console.WriteLine();
            }

            RequiresRedraw = false;
        }
        private static Point GetPlayerPosition()
        {
            for (var y = 0; y < YSize; y++)
            {
                for (var x = 0; x < XSize; x++)
                {
                    var spot = GetSpot(new Point(y, x));
                    if (ContainsPlayer(spot))
                    {
                        return new Point(y, x);
                    }
                }
            }

            return new Point(-1, -1);
        }

        private static char GetSpot(Point p)
        {
            if (p.X < 0 || p.X >= XSize || p.Y < 0 || p.Y >= YSize)
            {
                return 'E';
            }

            return Map[p.Y, p.X];
        }

        private static void Main(string[] args)
        {
            Map = LoadMap(@"C:\IronYard\sokoban_levels.txt", 0, out YSize, out XSize);

            while (true)
            {
                RenderWorld();
                RespondToUserInput();
                if (CheckForWin())
                {
                    RenderWorld();
                    System.Console.WriteLine("YOU WIN !!!!!!!");
                    break;
                }

            }

            System.Console.ReadLine();

        }

        private static char[,] LoadMap(string fileName, int mapNumber, out int ySize, out int xSize)
        {

            List<char[,]> maps = new List<char[,]>();
            string[] rows = File.ReadAllLines(fileName);

            List<char[]> currentMapRows = new List<char[]>();
            foreach (var row in rows)
            {

                if (string.IsNullOrWhiteSpace(row))
                {
                    xSize = currentMapRows.Max(x => x.Length);
                    ySize = currentMapRows.Count;

                    var currentMap = new char[ySize, xSize];
                    for (int i = 0; i < currentMapRows.Count; i++)
                    {
                        for (int j = 0; j < currentMapRows[i].Length; j++)
                        {
                            currentMap[i, j] = currentMapRows[i][j];
                        }
                    }

                    maps.Add(currentMap);

                    currentMapRows = new List<char[]>();
                }
                else
                {
                    currentMapRows.Add(row.ToCharArray());
                }

            }

            var loadMap = maps[mapNumber];

            ySize = loadMap.GetLength(0);
            xSize = loadMap.GetLength(1);

            return loadMap;

        }

        private static void RespondToUserInput()
        {
            var keyPressed = System.Console.ReadKey(true);
            switch (keyPressed.Key)
            {
                case ConsoleKey.LeftArrow:
                case ConsoleKey.UpArrow:
                case ConsoleKey.RightArrow:
                case ConsoleKey.DownArrow:
                    MoveBarrelOrPlayer(keyPressed.Key, GetPlayerPosition(), true);
                    break;
            }
        }

        private static void MoveBarrelOrPlayer(ConsoleKey key, Point point, bool isPlayer)
        {
            var newPosition = GetNewPosition(key, point);
            if (IsValidMove(key, newPosition, isPlayer))
            {
                UpdateOldAndNewSpots(point, newPosition, isPlayer);
                RequiresRedraw = true;
            }
        }

        private static void UpdateOldAndNewSpots(Point oldPoint, Point newPoint, bool isPlayer)
        {
            var oldSpot = GetSpot(oldPoint);
            var newSpot = GetSpot(newPoint);

            switch (oldSpot)
            {
                case '@':
                case 'o':
                    Map[oldPoint.Y, oldPoint.X] = ' ';
                    break;
                case '+':
                case '.':
                case '*':
                    Map[oldPoint.Y, oldPoint.X] = '.';
                    break;
            }


            switch (newSpot)
            {
                case ' ':
                    Map[newPoint.Y, newPoint.X] = isPlayer ? '@' : 'o';
                    break;
                case '.':
                    Map[newPoint.Y, newPoint.X] = isPlayer ? '+' : '*';
                    break;
            }

        }

        private static Point GetNewPosition(ConsoleKey key, Point newPoint)
        {
            var position = new Point(newPoint.Y, newPoint.X);
            switch (key)
            {
                case ConsoleKey.LeftArrow:
                    position.X -= 1;
                    break;
                case ConsoleKey.UpArrow:
                    position.Y -= 1;
                    break;
                case ConsoleKey.RightArrow:
                    position.X += 1;
                    break;
                case ConsoleKey.DownArrow:
                    position.Y += 1;
                    break;
            }

            return position;
        }

        private static bool IsValidMove(ConsoleKey keyPressed, Point newPoint, bool isPlayer)
        {
            var spot = GetSpot(newPoint);

            if (spot == 'E')
                return false;

            if (IsWall(spot))
                return false;

            if (IsEmptySpot(spot))
                return true;

            if (ContainsBarrel(spot))
            {
                if (!isPlayer)
                    return false;

                var newBarrelPosition = GetNewPosition(keyPressed, newPoint);
                if (IsValidMove(keyPressed, newBarrelPosition, false)) //can barrel move here?
                {
                    MoveBarrelOrPlayer(keyPressed, newPoint, false);
                    return true;
                }
            }

            return false;
        }

        private static bool IsWall(char spot)
        {
            return spot == '#';
        }

        private static bool IsEmptySpot(char spot)
        {
            return spot == ' ' || spot == '.';
        }

        private static bool ContainsBarrel(char spot)
        {
            return spot == 'o' || spot == '*';
        }

        private static bool ContainsPlayer(char spot)
        {
            return spot == '@' || spot == '+';
        }


    }

    public class Point
    {
        public Point(int y, int x)
        {
            Y = y;
            X = x;
        }

        public int Y { get; set; }
        public int X { get; set; }
    }
}