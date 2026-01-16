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

namespace aStarDemo
{
    public class Game1 : Microsoft.Xna.Framework.Game
    {
        GraphicsDeviceManager graphics;
        SpriteBatch spriteBatch;
        bool moveStart = false;
        bool moveEnd = false;
        public static bool searching = false;

        public Game1()
        {
            graphics = new GraphicsDeviceManager(this);
            Content.RootDirectory = "Content";
        }

        protected override void Initialize()
        {
            base.Initialize();
            IsMouseVisible = true;
        }

        Tile[,] tiles;
        Texture2D tileImage;
        TileGraph graph;
        protected override void LoadContent()
        {
            spriteBatch = new SpriteBatch(GraphicsDevice);
            
            

            tileImage = Content.Load<Texture2D>("tile");

            tiles = new Tile[GraphicsDevice.Viewport.Width / tileImage.Width, GraphicsDevice.Viewport.Height / tileImage.Height];

            for (int i = 0; i < tiles.GetLength(0); i++)
            {
                for (int ii = 0; ii < tiles.GetLength(1); ii++)
                {
                    tiles[i, ii] = new Tile(tileImage, new Vector2(i*tileImage.Width, ii*tileImage.Height));
                }
            }

            graph = new TileGraph(tiles.GetLength(0), tiles.GetLength(1));

            tiles[5, 5].State = TileState.StartTile;
            tiles[19, 11].State = TileState.EndTile;
            graph.Search(tiles);

            //Vector2 neighborTile = new Vector2(10, 5);
            //for(int i = 0; i < graph.GetTileNeighbors(tiles, neighborTile).Count; i++)
            //{
            //    Tile test = graph.GetTileNeighbors(tiles, neighborTile)[i];
            //    graph.GetTileNeighbors(tiles, neighborTile)[i].State = TileState.SearchedTile;
            //}
        }

        protected override void UnloadContent()
        {
        }
        TimeSpan elapsedTime = new TimeSpan(0, 0, 0);
        protected override void Update(GameTime gameTime)
        {
            elapsedTime += gameTime.ElapsedGameTime;
            InputManager.Update();
            for (int i = 0; i < tiles.GetLength(0); i++)
            {
                for (int ii = 0; ii < tiles.GetLength(1); ii++)
                {
                    tiles[i, ii].Update(gameTime, moveStart, moveEnd);
                    moveStart = tiles[i, ii].MoveStartTile(moveStart, moveEnd);
                    moveEnd = tiles[i, ii].MoveEndTile(moveStart, moveEnd);
                }
            }
            if (elapsedTime.Milliseconds > 500)
            {
                graph.bfsSearch(tiles);
            }
            base.Update(gameTime);
        }

        protected override void Draw(GameTime gameTime)
        {
            GraphicsDevice.Clear(Color.CornflowerBlue);

            spriteBatch.Begin();

            for (int i = 0; i < tiles.GetLength(0); i++)
            {
                for (int ii = 0; ii < tiles.GetLength(1); ii++)
                {
                    tiles[i, ii].Draw(spriteBatch);
                }
            }
            
            spriteBatch.End();

            base.Draw(gameTime);
        }
    }
}