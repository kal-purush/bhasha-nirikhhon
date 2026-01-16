using Microsoft.Xna.Framework;
using Microsoft.Xna.Framework.Graphics;
using SeaBattle.Common.Session;

namespace SeaBattle.StaticObjects
{
    public class GameplayBackground
    {
        public Texture2D BackgroundTexture;
        public Rectangle BackgrounRectangle1;
        public Rectangle BackgrounRectangle2;
        public Rectangle BackgrounRectangle3;
        public Rectangle BackgrounRectangle4;

        private bool _isFirstVerticalPosition;
        private bool _isFirstHorizontallyPosition;

        public GameplayBackground(Texture2D backgroundTexture, Point currentCoordinates)
        {
            BackgroundTexture = backgroundTexture;

            BackgrounRectangle1 = new Rectangle(currentCoordinates.X - Constants.LevelWidth, currentCoordinates.Y - Constants.LevelHeigh, Constants.LevelWidth, Constants.LevelHeigh);
            BackgrounRectangle2 = new Rectangle(currentCoordinates.X, currentCoordinates.Y - Constants.LevelHeigh, Constants.LevelWidth, Constants.LevelHeigh);
            BackgrounRectangle3 = new Rectangle(currentCoordinates.X, currentCoordinates.Y, Constants.LevelWidth, Constants.LevelHeigh);
            BackgrounRectangle4 = new Rectangle(currentCoordinates.X - Constants.LevelWidth, currentCoordinates.Y, Constants.LevelWidth, Constants.LevelHeigh);

            _isFirstVerticalPosition = true;
            _isFirstHorizontallyPosition = true;
        }

        public void Draw(SpriteBatch spriteBatch)
        {
            spriteBatch.Draw(BackgroundTexture, BackgrounRectangle1, Color.White);
            spriteBatch.Draw(BackgroundTexture, BackgrounRectangle2, Color.White);
            spriteBatch.Draw(BackgroundTexture, BackgrounRectangle3, Color.White);
            spriteBatch.Draw(BackgroundTexture, BackgrounRectangle4, Color.White);
        }

        public void Update(Point currentCoords)
        {
            UpdateHorizontally(currentCoords);
            UpdateVertical(currentCoords);
        }

        private void UpdateHorizontally(Point currentCoords)
        {
            if (_isFirstHorizontallyPosition)
            {
                if (currentCoords.X > BackgrounRectangle2.Center.X)
                {
                    BackgrounRectangle1 = new Rectangle(BackgrounRectangle1.X + 2 * Constants.LevelWidth, BackgrounRectangle1.Y, Constants.LevelWidth, Constants.LevelHeigh);
                    BackgrounRectangle4 = new Rectangle(BackgrounRectangle4.X + 2 * Constants.LevelWidth, BackgrounRectangle4.Y, Constants.LevelWidth, Constants.LevelHeigh);
                    _isFirstHorizontallyPosition = !_isFirstHorizontallyPosition;
                }
                else if (currentCoords.X < BackgrounRectangle1.Center.X)
                {
                    BackgrounRectangle2 = new Rectangle(BackgrounRectangle2.X - 2 * Constants.LevelWidth, BackgrounRectangle2.Y, Constants.LevelWidth, Constants.LevelHeigh);
                    BackgrounRectangle3 = new Rectangle(BackgrounRectangle3.X - 2 * Constants.LevelWidth, BackgrounRectangle3.Y, Constants.LevelWidth, Constants.LevelHeigh);
                    _isFirstHorizontallyPosition = !_isFirstHorizontallyPosition;
                }
            }
            else
            {
                if (currentCoords.X > BackgrounRectangle1.Center.X)
                {
                    BackgrounRectangle2 = new Rectangle(BackgrounRectangle2.X + 2 * Constants.LevelWidth, BackgrounRectangle2.Y, Constants.LevelWidth, Constants.LevelHeigh);
                    BackgrounRectangle3 = new Rectangle(BackgrounRectangle3.X + 2 * Constants.LevelWidth, BackgrounRectangle3.Y, Constants.LevelWidth, Constants.LevelHeigh);
                    _isFirstHorizontallyPosition = !_isFirstHorizontallyPosition;
                }
                else if (currentCoords.X < BackgrounRectangle2.Center.X)
                {
                    BackgrounRectangle1 = new Rectangle(BackgrounRectangle1.X - 2 * Constants.LevelWidth, BackgrounRectangle1.Y, Constants.LevelWidth, Constants.LevelHeigh);
                    BackgrounRectangle4 = new Rectangle(BackgrounRectangle4.X - 2 * Constants.LevelWidth, BackgrounRectangle4.Y, Constants.LevelWidth, Constants.LevelHeigh);
                    _isFirstHorizontallyPosition = !_isFirstHorizontallyPosition;
                }
            }
        }

        private void UpdateVertical(Point currentCoords)
        {
            if (_isFirstVerticalPosition)
            {
                if (currentCoords.Y < BackgrounRectangle1.Center.Y)
                {
                    BackgrounRectangle3 = new Rectangle(BackgrounRectangle3.X, BackgrounRectangle3.Y - 2 * Constants.LevelHeigh, Constants.LevelWidth, Constants.LevelHeigh);
                    BackgrounRectangle4 = new Rectangle(BackgrounRectangle4.X, BackgrounRectangle4.Y - 2 * Constants.LevelHeigh, Constants.LevelWidth, Constants.LevelHeigh);
                    _isFirstVerticalPosition = !_isFirstVerticalPosition;
                }
                else if (currentCoords.Y > BackgrounRectangle4.Center.Y)
                {
                    BackgrounRectangle1 = new Rectangle(BackgrounRectangle1.X, BackgrounRectangle1.Y + 2 * Constants.LevelHeigh, Constants.LevelWidth, Constants.LevelHeigh);
                    BackgrounRectangle2 = new Rectangle(BackgrounRectangle2.X, BackgrounRectangle2.Y + 2 * Constants.LevelHeigh, Constants.LevelWidth, Constants.LevelHeigh);
                    _isFirstVerticalPosition = !_isFirstVerticalPosition;
                }
            }
            else
            {
                if (currentCoords.Y < BackgrounRectangle4.Center.Y)
                {
                    BackgrounRectangle1 = new Rectangle(BackgrounRectangle1.X, BackgrounRectangle1.Y - 2 * Constants.LevelHeigh, Constants.LevelWidth, Constants.LevelHeigh);
                    BackgrounRectangle2 = new Rectangle(BackgrounRectangle2.X, BackgrounRectangle2.Y - 2 * Constants.LevelHeigh, Constants.LevelWidth, Constants.LevelHeigh);
                    _isFirstVerticalPosition = !_isFirstVerticalPosition;
                }
                else if (currentCoords.Y > BackgrounRectangle1.Center.Y)
                {
                    BackgrounRectangle3 = new Rectangle(BackgrounRectangle3.X, BackgrounRectangle3.Y + 2 * Constants.LevelHeigh, Constants.LevelWidth, Constants.LevelHeigh);
                    BackgrounRectangle4 = new Rectangle(BackgrounRectangle4.X, BackgrounRectangle4.Y + 2 * Constants.LevelHeigh, Constants.LevelWidth, Constants.LevelHeigh);
                    _isFirstVerticalPosition = !_isFirstVerticalPosition;
                }
            }
        }

        /*
        //  0 - ничего не делать
        //  1 - нарисовать справа
        // -1 - нарисовать слева
        private int CheckHorizontallyPosition(Point currentCoords)
        {
            if (_isFirstHorizontallyPosition)
            {
                if (currentCoords.X > BackgrounRectangle2.Center.X)
                {
                    return 1;
                }
                if (currentCoords.X < BackgrounRectangle1.Center.X)
                {
                    return -1;
                }
            }
            else
            {
                if (currentCoords.X > BackgrounRectangle1.Center.X)
                {
                    return 1;
                }
                if (currentCoords.X < BackgrounRectangle2.Center.X)
                {
                    return -1;
                }
            }
            return 0;
        }

        //  0 - ничего не делать
        //  1 - нарисовать сверху
        // -1 - нарисовать снизу
        private int CheckVerticalPosition(Point currentCoords)
        {
            if (_isFirstVerticalPosition)
            {
                if (currentCoords.Y < BackgrounRectangle1.Center.Y)
                {
                    return 1;
                }
                if (currentCoords.Y > BackgrounRectangle4.Center.Y)
                {
                    return -1;
                }
            }
            else
            {
                if (currentCoords.Y < BackgrounRectangle4.Center.Y)
                {
                    return 1;
                }
                if (currentCoords.Y > BackgrounRectangle1.Center.Y)
                {
                    return -1;
                }
            }
            return 0;

        }*/
    }
}