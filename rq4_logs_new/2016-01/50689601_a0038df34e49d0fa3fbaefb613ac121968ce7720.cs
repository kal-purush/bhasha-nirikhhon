using Microsoft.Xna.Framework.Graphics;
using Microsoft.Xna.Framework;

namespace GameJam2016
{
    public class AnimatedSprite
    {
        public Texture2D Texture { get; set; }
        public int Rows { get; set; }
        public int Columns { get; set; }
        public int currentFrame;
        private int totalFrames;
        private int currentUpdate;
        private int updatesPerFrame = 5;

        public Vector2[] Animation;
        int currentAnimationRow;
        int currentAnimationCollumn;

        public AnimatedSprite(Texture2D texture, int rows, int columns, Vector2 [] animation)
        {
            Texture = texture;
            Rows = rows;
            Columns = columns;
            currentFrame = 0;
            totalFrames = Rows * Columns;
            Animation = animation;
        }

        public void Update(GameTime gameTime)
        {
            currentUpdate++;
            if (currentUpdate == updatesPerFrame)
            {
                currentUpdate = 0;

                currentFrame++;
                    if (currentFrame >= Animation.Length)
                    currentFrame = 0;
                currentAnimationRow = (int)Animation[currentFrame].X;
                currentAnimationCollumn = (int)Animation[currentFrame].Y;
                
            }
        }

        public void Draw(SpriteBatch spriteBatch, Vector2 location)
        {
            int width = Texture.Width / Columns;
            int height = Texture.Height / Rows;
            //int row = (int)((float)currentFrame / (float)Columns);
            int row = currentAnimationRow;
            int column = currentAnimationCollumn;

            Rectangle sourceRectangle = new Rectangle(width * column, height * row, width, height);
            Rectangle destinationRectangle = new Rectangle((int)location.X, (int)location.Y, width, height);

            spriteBatch.Begin();
            spriteBatch.Draw(Texture, destinationRectangle, sourceRectangle, Color.White);
            spriteBatch.End();
        }
    }
}