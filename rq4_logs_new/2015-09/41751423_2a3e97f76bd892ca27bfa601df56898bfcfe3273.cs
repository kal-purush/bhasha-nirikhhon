using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Microsoft.Xna.Framework;
//using Microsoft.Xna.Framework.Audio;
//using Microsoft.Xna.Framework.Content;
//using Microsoft.Xna.Framework.GamerServices;
using Microsoft.Xna.Framework.Graphics;
//using Microsoft.Xna.Framework.Input;
//using Microsoft.Xna.Framework.Media;

namespace mst_boredom_remover
{
    class Map : UIObject
    {
        private Vector2 position;
        private List<Texture2D> tiles;
        private char[,] map;

        private const int MAP_X = 1280 / 2;
        private const int MAP_Y = 720 / 2;
        private const int TILE_PX_SIZE = 28;
        private const int RES_X = 1280;
        private const int RES_Y = 720;
        private bool smallMode = false;
        private Vector2 tileIndex;

        struct BiomeInfo
        {
            public char Type;
            public int X;
            public int Y;
        };

        public Map(Vector2 position, List<Texture2D> tiles, Texture2D texture = null) 
            : base(texture, position)
        {
            this.position = position;
            this.tiles = tiles;
            //this.map = map;
            //this.texture = texture;

            tileIndex = Vector2.Zero;

            generator();
        }

        public override void changeContext(int id)
        {
            //base.changeContext(id);

            if (id == 2)
            {
                // small mode
                smallMode = true;
            }
            else
            {
                // regular mode
                smallMode = false;
            }
        }

        public override void mapMove(int deltaX, int deltaY)
        {
            tileIndex.X += deltaX;
            tileIndex.Y += deltaY;
            //base.mapMove(deltaX, deltaY);
        }

        private void generator()
        {
            const int NumBio = 140;
            Random r = new Random();
            BiomeInfo[] bio = new BiomeInfo[NumBio];
            for (int i = 0; i < bio.Length; i++)
                bio[i] = new BiomeInfo();
            for (int a = 0; a < (NumBio / 7); a++)
            {
                bio[a * 7].Type = '~'; //Ocean
                bio[a * 7].X = r.Next(0, 100);
                bio[a * 7].Y = r.Next(0, 100);

                bio[a * 7 + 1].Type = '+';//Plain
                bio[a * 7 + 1].X = r.Next(0, 100);
                bio[a * 7 + 1].Y = r.Next(0, 100);

                bio[a * 7 + 2].Type = 'M';//Mountain
                bio[a * 7 + 2].X = r.Next(0, 100);
                bio[a * 7 + 2].Y = r.Next(0, 100);

                bio[a * 7 + 3].Type = 'F';//Forest
                bio[a * 7 + 3].X = r.Next(0, 100);
                bio[a * 7 + 3].Y = r.Next(0, 100);

                bio[a * 7 + 4].Type = '%';//Dreadlands
                bio[a * 7 + 4].X = r.Next(0, 100);
                bio[a * 7 + 4].Y = r.Next(0, 100);

                bio[a * 7 + 5].Type = 'D';//Desert
                bio[a * 7 + 5].X = r.Next(0, 100);
                bio[a * 7 + 5].Y = r.Next(0, 100);

                bio[a * 7 + 6].Type = 'T';//Tundra
                bio[a * 7 + 6].X = r.Next(0, 100);
                bio[a * 7 + 6].Y = r.Next(0, 100);
            }

            char[,] field = new char[MAP_Y, MAP_X];
            for (int i = 0; i < MAP_Y; i++)
            {
                for (int j = 0; j < MAP_X; j++)
                {
                    char nearest = '~';
                    int dist = 5000;
                    for (int z = 0; z < NumBio; z++)
                    {
                        int Xdiff = bio[z].X - i;
                        int Ydiff = bio[z].Y - j;
                        int Cdist = Xdiff * Xdiff + Ydiff * Ydiff;
                        if (Cdist < dist)
                        {
                            nearest = bio[z].Type;
                            dist = Cdist;
                        }

                    }
                    field[i, j] = nearest;
                }
            }
            for (int i = 0; i < MAP_Y; i++)
            {
                field[i, 0] = '~';
                field[0, i] = '~';
                field[i, 99] = '~';
                field[99, i] = '~';
            }
            //Adding Resources.
            for (int i = 0; i < MAP_Y; i++)
            {
                for (int j = 0; j < MAP_X; j++)
                {
                    if (field[i, j] == 'M')
                    {
                        if (r.Next(0, 150) % 150 <= 1)
                            field[i, j] = 'G';//inserts gold mine resource
                    }
                    else if (field[i, j] == 'F')
                    {
                        if (r.Next(0, 150) % 150 <= 1)
                            field[i, j] = 'L';//sawmill for lumber
                    }
                    else if (field[i, j] == '%')
                    {
                        if (r.Next(0, 150) % 150 <= 1)
                            field[i, j] = '*';//magic crystal resource
                    }

                }
            }
            //Outputting Map to Screen
            map = field;

        }

        public override void Update(GameTime gt)
        {
            //base.Update(gt);
        }

        public override void Draw(SpriteBatch sb)
        {
            // not small mode
            if (!smallMode)
            {
                // keep drawing until we run out of screen space, x and y
                for (int x = (int)position.X; x < (RES_X / TILE_PX_SIZE); x++)
                {
                    for (int y = (int)position.Y; y < (RES_Y / TILE_PX_SIZE); y++)
                    {
                        // 2D array draw tiles at their designated spots, in TILE_PX x TILE_PX squares
                        sb.Draw(tiles[charToInt(map[(int)tileIndex.X + x, (int)tileIndex.Y + y])],
                            new Rectangle((int)position.X + TILE_PX_SIZE * x, (int)position.Y + TILE_PX_SIZE * y, TILE_PX_SIZE, TILE_PX_SIZE), Color.White);
                    }
                }
            }
            // small mode for new game screen
            else
            {
                // different position
                position.X = 100;
                position.Y = 200;
                for (int x = (int)position.X; x < (RES_X - 100 / 2); x++)
                {
                    for (int y = (int)position.Y; y < (RES_Y - 200 / 2); y++)
                    {
                        /*sb.Draw(tiles[charToInt(map[x, y])],
                            new Rectangle((int)position.X + 2 * x, (int)position.Y + 2 * y, 2, 2), Color.White);
                         * */
                    }
                }
            }
            //base.Draw(sb);
        }

        private int charToInt(char c)
        {
            switch (c)
            {
                case '+':
                    return 1;
                    break;
                case 'M':
                    return 2;
                    break;
                case 'D':
                    return 3;
                    break;
                case '~':
                    return 4;
                    break;
                case '%':
                    return 5;
                    break;
                case 'T':
                    return 6;
                    break;
                case 'F':
                    return 7;
                    break;
                default:
                    // 0 is not a tile, more efficient than try-catch index out of bounds
                    return 0;
                    break;
            }
        }
    }
}