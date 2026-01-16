using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using SnakeDefender.GameEngine.GameObject;

namespace SnakeDefender.GameEngine.Test.AdditionalsClasses
{
    public class GameSettingsTest : IGameSettings
    {
        #region Public Property

        public int GameBoardWidth { get; set; }
        public int GameBoardHeight { get; set; }
        public int GameSpeed { get; set; }
        public double GameScore { get; set; }
        public int GamePoints { get; set; }
        public int SnakeLenght { get; set; }
        public int SnakePosX { get; set; }
        public int SnakePosY { get; set; }
        public Direction GameMoveDirection { get; set; }
        public string ResultPath { get; set; }

        #endregion

        #region Constructors

        public GameSettingsTest()
        {
            
        }
        public GameSettingsTest(int width, int height, 
            int speed, double score, int points,
            int snakeLenght, int posX, int posY, 
            Direction direction, string path)
        {
            GameBoardWidth = width;
            GameBoardHeight = height;
            GameSpeed = speed;
            GameScore = score;
            GamePoints = points;
            SnakeLenght = snakeLenght;
            SnakePosX = posX;
            SnakePosY = posY;
            GameMoveDirection = direction;
            ResultPath = path;
        }

        #endregion
    }
}