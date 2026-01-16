using System;
using SnakeDefender.GameEngine.GameObject;

namespace SnakeDefender.ConsoleUI.AdditionalClasses
{
    public class RandomGenerator : IRandomGenerator
    {
        #region Private fields

        private readonly int _boardWidth;
        private readonly int _boardHeight;

        #endregion

        #region Constructor
        public RandomGenerator(int boardWidth, int boardHeight)
        {
            this._boardWidth = boardWidth;
            this._boardHeight = boardHeight;
        }

        #endregion

        #region Public Method
        public Point Generate()
        {           
            var rnd = new Random();
            var genPoint = new Point(rnd.Next(0, this._boardWidth),
                rnd.Next(0, this._boardHeight));
            return genPoint;
        }

        #endregion
    }
}