using System;
using System.Collections.Generic;
using System.Threading;
using Microsoft.Xna.Framework;
using Microsoft.Xna.Framework.Graphics;
using Microsoft.Xna.Framework.Input;

namespace GSeoFinalProject
{
    /// <summary>
    /// This class store high score, and it shows a current score
    /// </summary>
    class Score
    {
        Game1 game;
        private int currentScore = 0;

        SpriteFont scoreWords;
        Vector2 scorePosition;
        List<int> scoreList = new List<int>();

        public int CurrentScore { get => currentScore; set => currentScore = value; }

        public Score(Game1 game)
        {
            this.game = game;
            scorePosition = new Vector2((game.GraphicsDevice.Viewport.Width - 200),0);

            scoreWords = game.Content.Load<SpriteFont>("Fonts/hilightFont");
        }
        public void Draw(SpriteBatch spriteBatch)
        {
            spriteBatch.DrawString(scoreWords, currentScore.ToString(), scorePosition, Color.White);
        }
            //to compare other high score in a file
    }
}