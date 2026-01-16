using System;
using System.Collections.Generic;
using System.Linq;
using Microsoft.Xna.Framework;
using Microsoft.Xna.Framework.Audio;
using Microsoft.Xna.Framework.Content;
using Microsoft.Xna.Framework.GamerServices;
using Microsoft.Xna.Framework.Graphics;
using Microsoft.Xna.Framework.Input;
using Microsoft.Xna.Framework.Media;

namespace Creare
{
    public class Area
    {
        public int[, ,] area = new int[60, 45, 2];
        public Vector2 difference = new Vector2((Global.SIZE_X / 2), 280); // distance in x,y from 0,0 in Global.world
        public Vector2 smallDifference = new Vector2((float)(1f / Global.ScreenWidth), (float)(1f / Global.ScreenHeight)); // decimal shift for the grid to draw. always between 0 and 1
        public Texture2D blank;
        public List<Texture2D> textures = new List<Texture2D>();
        public List<Texture2D> backgrounds = new List<Texture2D>();
        List<Texture2D> grass = new List<Texture2D>();
        public bool colliding = false;
        public int digIndex = 0;
        List<Texture2D> digTextures = new List<Texture2D>();
        double digTimer = 0;
        public Vector2 digLocation = new Vector2(-1, -1);

        public Area(Texture2D blank, List<Texture2D> digTextures)
        {
            this.blank = blank;
            this.digTextures = digTextures;
        }
        public void Update(GameTime gameTime, Player player)
        {
            // smooth movement
            #region Smooth Movement
            if (smallDifference.X >= 1)
            {
                difference.X--;
                smallDifference.X = (float)(1f / Global.ScreenWidth);
            }
            if (smallDifference.Y >= 1)
            {
                difference.Y--;
                smallDifference.Y = (float)(1f / Global.ScreenHeight);
            }
            if (smallDifference.X <= 0)
            {
                difference.X++;
                smallDifference.X = (float)((Global.ScreenWidth - 1f) / Global.ScreenWidth);
            }
            if (smallDifference.Y <= 0)
            {
                difference.Y++;
                smallDifference.Y = (float)((Global.ScreenHeight - 1f) / Global.ScreenHeight);
            }
            #endregion
            for (int y = 0; y < 45; y++)
            {
                for (int x = 0; x < 60; x++)
                {
                    if (x + difference.X <= Global.SIZE_X && x + difference.X > 0 && y + difference.Y <= Global.SIZE_Y && y + difference.Y > 0)
                    {
                        area[x, y, 0] = Global.world[x + (int)difference.X, y + (int)difference.Y, 0]; // update the index
                    }
                    else
                    {
                        area[x, y, 0] = -1; // if edge, blank space
                    }
                    /*try
                    {
                        
                    } // try
                    catch
                    {
                        area[x, y, 0] = -1; // if edge, blank space
                    } // catch*/
                } // for x
            } // for y

            // collision detection
            Rectangle playerBounds = new Rectangle(397, 328, 6, 1);
            bool ground = Collision(playerBounds, player);

            playerBounds = new Rectangle(390, 280, 1, 30);
            bool left = Collision(playerBounds, player);

            playerBounds = new Rectangle(405, 280, 1, 34);
            bool right = Collision(playerBounds, player);

            playerBounds = new Rectangle(397, 281, 6, 1);
            bool top = Collision(playerBounds, player);

            if (ground)
            {
                colliding = true;
                player.isFalling = false;
            }
            else
            {
                colliding = false;
                player.isFalling = true;
            }
            if (left)
            {
                player.canMoveLeft = false;
            }
            else
            {
                player.canMoveLeft = true;
            }
            if (right)
            {
                player.canMoveRight = false;
            }
            else
            {
                player.canMoveRight = true;
            }
            if (top)
            {
                player.jumpTimer = 0;
            }
        }
        public void Draw(SpriteBatch spriteBatch)
        {
            for (int y = 0; y < 45; y++)
            {
                for (int x = 0; x < 60; x++)
                {
                    if (area[x, y, 0] == -1)
                    {
                        // draw the void
                        spriteBatch.Draw(blank, new Vector2((smallDifference.X + (x * 16)), (smallDifference.Y + (y * 16))), Color.White);
                    }
                    else if (area[x,y,0] == 0 && (y + difference.Y) > 450) // if background and deep
                    {
                        // draw cave background
                        spriteBatch.Draw(backgrounds[1], new Vector2(-86f + ((smallDifference.X * 16f) + ((float)x * 16f)), -86f + ((smallDifference.Y * 16f) + ((float)y * 16f))), Color.White);
                    }
                    else
                    {
                        // draw the block
                        spriteBatch.Draw(textures[area[x, y, 0]], new Vector2(-86f + ((smallDifference.X * 16f) + ((float)x * 16f)), -86f + ((smallDifference.Y * 16f) + ((float)y * 16f))), Color.White);
                        if (area[x, y, 0] >= 1 && area[x, y, 0] <= 3) // if dirt
                        {
                            if (y > 0)
                            {
                                if (area[x, y - 1, 0] == 0) // top
                                {
                                    //spriteBatch.Draw(grass[1], new Vector2(-86f + (smallDifference.X * 16f) + ((float)x * 16f), -86f + (smallDifference.Y * 16f) + ((float)y * 16f)), Color.White);
                                }
                            }
                            if (y < 44)
                            {
                                if (area[x, y + 1, 0] == 0) // bottom
                                {
                                    //spriteBatch.Draw(grass[1], new Vector2(-86f + (smallDifference.X * 16f) + ((float)x * 16f), -86f + (smallDifference.Y * 16f) + ((float)y * 16f) + 14f), Color.White);
                                }
                            }
                            if (x > 0)
                            {
                                if (area[x - 1, y, 0] == 0) // left
                                {//top one is the good one. Ignore the bottom one. He stole the other ones cookies.
                                    //spriteBatch.Draw(grass[0], new Rectangle((int)(-86f + (smallDifference.X * 16f) + (float)x * 16f), (int)(-86f + (smallDifference.Y * 16f) + (float)y * 16f), 4 ,16), null, Color.White, 0, new Vector2(0,0), SpriteEffects.FlipHorizontally, 0f);
                                }
                            }
                            if (x < 59)
                            {
                                if (area[x + 1, y, 0] == 0) // right
                                {
                                    //spriteBatch.Draw(grass[0], new Vector2(-86f + (smallDifference.X * 16f) + ((float)x * 16f) + 14f, -86f + (smallDifference.Y * 16f) + ((float)y * 16f)), Color.White);
                                }
                            }
                        }
                        if (Global.debugMode)
                        {
                            spriteBatch.Draw(Global.grid, new Vector2(-86f + ((smallDifference.X * 16f) + ((float)x * 16f)), -86f + ((smallDifference.Y * 16f) + ((float)y * 16f))), Color.White);
                        }
                        
                    }
                    
                } // for
            } // for
            Vector2 tempMouse = MouseCoord();
            if (Global.digging == true && MouseInRange() && area[(int)tempMouse.X, (int)tempMouse.Y, 0] != 0)
            {
                spriteBatch.Draw(digTextures[digIndex], new Vector2((-86f + (digLocation.X * 16f) + (smallDifference.X * 16f)), (-86f + (digLocation.Y * 16f) + (smallDifference.Y * 16f))), Color.White);
            }
            //if (Global.leftMouse)
            //{
            //    spriteBatch.Draw(digTextures[3], new Vector2(- 86f + (Global.blockBeingDug.X * 16f) + (smallDifference.X * 16f), -86f + (Global.blockBeingDug.Y * 16f) + (smallDifference.Y * 16f)), Color.White);
            //}
        } // draw
        public bool Collision(Rectangle playerBounds, Player player)
        {
            // collision detection
         //   if (!Global.debugMode)
         //   {
                Rectangle blockBounds;
                for (int y = 19; y < 26; y++)
                {
                    for (int x = 24; x < 33; x++)
                    {
                        blockBounds = new Rectangle((-86 + ((x) * 16) + (int)(smallDifference.X * (float)16)), -86 + (y * 16), 16, 16);
                        if (area[x, y, 0] != 0)
                        {
                            if (playerBounds.Intersects(blockBounds))
                            {
                                return true;
                            }
                            else
                            {
                                continue;
                            }
                        }
                    }
                }
                return false;
         //   }
            return false;
        }
        public void BlockModifier(string situation, Player player, GameTime gameTime)
        {
            bool dig = false;
            digTimer += gameTime.ElapsedGameTime.TotalSeconds;
            if (digTimer < .5 && digTimer > 0)
            {
                digIndex = 0;
            }
            else if(digTimer < 1.0 && digTimer > .5)
            {
                digIndex = 1;
            }
            else if (digTimer < 1.5 && digTimer > 1.0)
            {
                digIndex = 2;
            }
            else if (digTimer < 2 && digTimer > 1.5)
            {
                digIndex = 3;
                dig = true;
            }
            
            Random random = new Random();
            int MouseCordX;
            int MouseCordY;
            MouseState mouse = Mouse.GetState();

            //Vector2 mouseDistance; // calculate distance from player's center
            //mouseDistance.X = Math.Abs(mouse.X - (player.playerPosition.X + 16));
            //mouseDistance.Y = Math.Abs(mouse.Y - (player.playerPosition.Y + 24));

            //MouseCordX = (((mouse.X + 86) - (int)(smallDifference.X * 16f)) / 16);
            //MouseCordY = (((mouse.Y + 86) - (int)(smallDifference.Y * 16f)) / 16);
            Vector2 MouseCord = MouseCoord();
            digLocation = new Vector2(MouseCord.X, MouseCord.Y);
            
            if (dig || situation == "Add")
            {
                //int BlockID = 1; // temprorary hardcoded value
                int BlockID = 0;
                // prevents the player from placing/removing blocks that are far away
                if (MouseInRange())
                {
                    switch (situation)
                    {
                        case "Delete":
                            digTimer = 0;
                            digLocation.X = -1;
                            digLocation.Y = -1;
                            // 28, 21
                            BlockID = MouseIndex();
                            player.InvAdd(BlockID);
                            Global.world[(int)(MouseCord.X + difference.X), (int)(MouseCord.Y + difference.Y), 0] = 0;
                            break;
                        case "Add":                            
                            BlockID = player.Inv[player.selectedInvSlot, 0];
                            if (BlockID != 0)
                            {
                                if (player.InvDel(BlockID) && area[(int)MouseCord.X, (int)MouseCord.Y, 0] == 0)
                                {
                                    if ((BlockID == 1))//Dirt
                                    {
                                        BlockID = random.Next(1, 4);
                                    }
                                    else if (BlockID == 4)
                                    {
                                        BlockID = random.Next(4, 7);
                                    }
                                    Global.world[(int)(MouseCord.X + difference.X), (int)(MouseCord.Y + difference.Y), 0] = BlockID;
                                }
                            }
                            break;
                    }
                }
            }
        }
        public bool MouseInRange()
        {
            MouseState mouse = Mouse.GetState();
            Vector2 mouseDistance = Vector2.Zero;
            mouseDistance.X = Math.Abs(mouse.X - (386 + 16));
            mouseDistance.Y = Math.Abs(mouse.Y - (305));

            if (mouseDistance.X <= 16 * 4 && mouseDistance.Y < 16 * 4)
            {
                return true;
            }
            else
            {
                return false;
            }
        }
        public int MouseIndex()
        {
            MouseState mouse = Mouse.GetState();
            Vector2 MouseCord = Vector2.Zero;

            MouseCord.X = (((mouse.X + 86) - (int)(smallDifference.X * 16f)) / 16);
            MouseCord.Y = (((mouse.Y + 86) - (int)(smallDifference.Y * 16f)) / 16);
            if (Global.world[(int)(MouseCord.X + difference.X), (int)(MouseCord.Y + difference.Y), 0] != -1)
            {
                return Global.world[(int)(MouseCord.X + difference.X), (int)(MouseCord.Y + difference.Y), 0];
            }
            else
            {
                return 0;
            }
        }
        public Vector2 MouseCoord()
        {
            MouseState mouse = Mouse.GetState();
            Vector2 MouseCord = Vector2.Zero;

            MouseCord.X = (((mouse.X + 86) - (int)(smallDifference.X * 16f)) / 16);
            MouseCord.Y = (((mouse.Y + 86) - (int)(smallDifference.Y * 16f)) / 16);

            return MouseCord;
        }
        public void AddTexture(Texture2D texture)
        {
            textures.Add(texture);
        }
        public void AddGrassVariety(Texture2D texture)
        {
            grass.Add(texture);
        }
        public void AddBackground(Texture2D texture)
        {
            backgrounds.Add(texture);
        }
        public void LoadData(Vector2 difference, Vector2 smallDifference)
        {
            this.difference = difference;
            this.smallDifference = smallDifference;
        }
        public void ResetDig()
        {
            Global.digging = false;
            digTimer = 0;
            digLocation.X = -1;
            digLocation.Y = -1;
        }
        public void Reset()
        {
            this.colliding = false;
            this.difference = Vector2.Zero;
            this.smallDifference = Vector2.Zero;
            ResetDig();
        }
    } // class area
} // end of the world