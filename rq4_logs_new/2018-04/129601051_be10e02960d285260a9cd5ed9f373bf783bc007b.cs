using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.IO;

namespace Wow
{
    public enum Tile
    {
        Dirt = 0,
        Rock,
        Food,
        Wall
    }

    public class Game
    {
        public const int MapWidth = 20;
        public const int MapHeight = 15;
        public Tile?[,] Map { get; private set; }
        public int WilliX { get; private set; } = -1;
        public int WilliY { get; private set; } = -1;
        public bool WilliDead { get; private set; }
        
        public Game(string filename)
        {
            StreamReader reader = new StreamReader(filename);
            Map = new Tile?[MapWidth, MapHeight];
            for (int y = 0; y < MapHeight; y++)                
            {
                for (int x = 0; x < MapWidth; x++)
                {
                    char TileChar = (char)reader.Read();
                    if (TileChar == '\r') TileChar = (char)reader.Read();
                    if (TileChar == '\n') TileChar = (char)reader.Read();
                    switch(TileChar)
                    {
                        case 'd': Map[x, y] = Tile.Dirt; break;
                        case 'r': Map[x, y] = Tile.Rock; break;
                        case 'f': Map[x, y] = Tile.Food; break;
                        case 'w': Map[x, y] = Tile.Wall; break;
                        case ' ': Map[x, y] = null; break;
                        case 's':
                            Map[x, y] = null;
                            WilliX = x;
                            WilliY = y;
                            break;
                        default:
                            throw new InvalidDataException("Unrecognised character '" + TileChar + "' in map.");
                    }
                }
            }
            if(WilliX == -1)
            {
                throw new InvalidDataException("No Willi starting position given. Use 's'.");
            }
            WilliDead = false;
        }

        /// <summary>
        /// If a tile is solid, the player cannot walk through it
        /// </summary>
        /// <param name="x"></param>
        /// <param name="y"></param>
        /// <returns></returns>
        private bool IsSolid(int x, int y)
        {
            return Map[x, y] == Tile.Rock
                || Map[x, y] == Tile.Wall
                || x == 0 || y == 0
                || x == MapWidth || y == MapHeight;
        }

        /// <summary>
        /// If a tile is smooth, a rock can fall through it
        /// </summary>
        /// <param name="x"></param>
        /// <param name="y"></param>
        /// <returns></returns>
        private bool IsSmooth(int x, int y)
        {
            return Map[x, y] == null && (WilliX != x || WilliY != y);
        }

        /// <summary>
        /// Update a column by making rocks fall
        /// </summary>
        /// <param name="x">The column to update</param>
        /// <param name="y">The starting y value</param>
        private void UpdateColumn(int x, int y)
        {
            // If we're on map edge, do nothing
            if (y == MapHeight - 1) return;

            // If we're not on a rock, try the next column
            if (Map[x, y] != Tile.Rock) { UpdateColumn(x, y + 1); return; }
            
            // If we're sitting on a rock, update that first
            if (Map[x, y + 1] == Tile.Rock)
            {
                UpdateColumn(x, y + 1);
            }

            // If we are on a rock, see how far we can drop.
            int drop = 1;
            while (IsSmooth(x, y + drop)) drop++;
            Map[x, y] = null;
            Map[x, y + drop - 1] = Tile.Rock;
            if (drop > 1 && y + drop == WilliY)
            {
                WilliDead = true;
            }
        }

        public bool GameFinished()
        {
            for(int x = 0; x < MapWidth; x++)
            {
                for (int y = 0; y < MapHeight; y++)
                {
                    if (Map[x, y] == Tile.Food) return false;
                }
            }
            return true;
        }

        private void TryWalk(int x, int y)
        {
            if (IsSolid(x, y)) return;
            int oldx = WilliX;
            WilliX = x;
            WilliY = y;
            Map[x, y] = null; // Swallow 
            UpdateColumn(oldx, 0);
        }

        public void WalkUp()    { TryWalk(WilliX, WilliY - 1); }
        public void WalkRight() { TryWalk(WilliX + 1, WilliY); }
        public void WalkDown()  { TryWalk(WilliX, WilliY + 1); }
        public void WalkLeft()  { TryWalk(WilliX - 1, WilliY); }
    }
}