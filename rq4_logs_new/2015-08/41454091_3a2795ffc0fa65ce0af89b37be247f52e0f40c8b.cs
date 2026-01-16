using System;

namespace Sokoban.Console
{
   public class ConsoleGameRender : IGameRender
    {
        public void Draw(World _map)
        {
            System.Console.Clear();
            System.Console.BackgroundColor = ConsoleColor.Black;
            for (var i = 0; i < _map.YSize; i++)
            {
                for (var j = 0; j < _map.XSize; j++)
                {
                    System.Console.ForegroundColor = ConsoleColor.White;
                    switch (_map.GetTile(i, j))
                    {
                        case TileType.Wall:
                            System.Console.ForegroundColor = ConsoleColor.Green;
                            var up = _map.GetTile(i - 1, j) == TileType.Wall;
                            var down = _map.GetTile(i + 1, j) == TileType.Wall;
                            var left = _map.GetTile(i, j - 1) == TileType.Wall;
                            var right = _map.GetTile(i, j + 1) == TileType.Wall;

                            if (up && down && left && right)
                            {
                                System.Console.Write('╬');
                                break;
                            }

                            if (up && down && left)
                            {
                                System.Console.Write('╣');
                                break;
                            }

                            if (up && down && right)
                            {
                                System.Console.Write('╠');
                                break;
                            }

                            if (up && left && right)
                            {
                                System.Console.Write('╩');
                                break;
                            }

                            if (down && left && right)
                            {
                                System.Console.Write('╦');
                                break;
                            }

                            if (down && right)
                            {
                                System.Console.Write('╔');
                                break;
                            }

                            if (down && left)
                            {
                                System.Console.Write('╗');
                                break;
                            }

                            if (up && left)
                            {
                                System.Console.Write('╝');
                                break;
                            }

                            if (up && right)
                            {
                                System.Console.Write('╚');
                                break;
                            }

                            if (up || down)
                            {
                                System.Console.Write('║');
                                break;
                            }


                            if (left || right)
                            {
                                System.Console.Write('═');
                                break;
                            }

                            System.Console.Write('X');

                            break;
                        case TileType.Blank:
                            System.Console.Write(' ');
                            break;
                        case TileType.Barrel:
                            System.Console.ForegroundColor = ConsoleColor.Blue;
                            System.Console.Write('o');
                            break;
                        case TileType.Storage:
                            System.Console.ForegroundColor = ConsoleColor.Red;
                            System.Console.Write('.');
                            break;
                        case TileType.Player:
                        case TileType.PlayerOverStorage:
                            System.Console.ForegroundColor = ConsoleColor.White;
                            System.Console.Write('☻');
                            break;
                        case TileType.BarrelInStorage:
                            System.Console.ForegroundColor = ConsoleColor.Blue;
                            System.Console.Write('Θ');
                            break;
                    }
                }
                System.Console.WriteLine();
            }
        }

        public void DrawWin()
        {
            System.Console.WriteLine("YOU WIN !!!!!!!");
        }
    }
}