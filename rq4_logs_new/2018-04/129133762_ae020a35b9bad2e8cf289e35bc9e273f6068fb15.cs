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

namespace MazeGame
{
    /// <summary>
    /// This is the main type for your game
    /// </summary>
    public class Game1 : Microsoft.Xna.Framework.Game
    {
        GraphicsDeviceManager graphics;
        SpriteBatch spriteBatch;

        Tile[,] tileMap;
        Texture2D allPurposeTexture;

        const int TileSize = 20;

        public Game1()
        {
            graphics = new GraphicsDeviceManager(this);
            Content.RootDirectory = "Content";

            graphics.PreferredBackBufferHeight = 1000;
            graphics.PreferredBackBufferWidth = 1000;
            graphics.ApplyChanges();
        }

        /// <summary>
        /// Allows the game to perform any initialization it needs to before starting to run.
        /// This is where it can query for any required services and load any non-graphic
        /// related content.  Calling base.Initialize will enumerate through any components
        /// and initialize them as well.
        /// </summary>
        protected override void Initialize()
        {
            // TODO: Add your initialization logic here
            tileMap = new Tile[TileSize, TileSize];

           

            base.Initialize();
        }

        /// <summary>
        /// LoadContent will be called once per game and is the place to load
        /// all of your content.
        /// </summary>
        protected override void LoadContent()
        {
            // Create a new SpriteBatch, which can be used to draw textures.
            spriteBatch = new SpriteBatch(GraphicsDevice);

            // TODO: use this.Content to load your game content here
            allPurposeTexture = this.Content.Load<Texture2D>("white");

            for (int row = 0; row < tileMap.GetLength(0); row++)
            {
                for (int column = 0; column < tileMap.GetLength(1); column++)
                {
                    Random r = new Random();
                    if (row % 2 == 0)
                    {
                        makeTile(row, column, allPurposeTexture, new Color((int)r.Next(50, 150), (int)r.Next(50, 150), (int)r.Next(50, 150)));
                    }
                    else
                    {
                        makeTile(row, column, allPurposeTexture, Color.Black);
                    }
                    if(column % 2 == 0)
                    {
                        makeTile(row, column, allPurposeTexture, Color.Purple);
                    }
                }
            }
        }

        private void makeTile(int row, int column, Texture2D texture, Color color)
        {
            tileMap[row, column] = new Tile(
                           new Rectangle(row * (GraphicsDevice.Viewport.Width / TileSize), column * (GraphicsDevice.Viewport.Height / TileSize),
                                                GraphicsDevice.Viewport.Width / TileSize, GraphicsDevice.Viewport.Height / TileSize), texture, color);
        }

        /// <summary>
        /// UnloadContent will be called once per game and is the place to unload
        /// all content.
        /// </summary>
        protected override void UnloadContent()
        {
            // TODO: Unload any non ContentManager content here
        }

        /// <summary>
        /// Allows the game to run logic such as updating the world,
        /// checking for collisions, gathering input, and playing audio.
        /// </summary>
        /// <param name="gameTime">Provides a snapshot of timing values.</param>
        protected override void Update(GameTime gameTime)
        {
            // Allows the game to exit
            if (GamePad.GetState(PlayerIndex.One).Buttons.Back == ButtonState.Pressed)
                this.Exit();

            // TODO: Add your update logic here

            base.Update(gameTime);
        }

        /// <summary>
        /// This is called when the game should draw itself.
        /// </summary>
        /// <param name="gameTime">Provides a snapshot of timing values.</param>
        protected override void Draw(GameTime gameTime)
        {
            GraphicsDevice.Clear(Color.CornflowerBlue);

            // TODO: Add your drawing code here
            spriteBatch.Begin();
            for (int row = 0; row < tileMap.GetLength(0); row++)
            {
                for (int column = 0; column < tileMap.GetLength(1); column++)
                {
                    spriteBatch.Draw(tileMap[row, column].TileTexture, tileMap[row, column].TileRect, tileMap[row, column].TileColor);
                }
            }
            spriteBatch.End();

            base.Draw(gameTime);
        }
    }
}