using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace AA
{
    public class GameLevels
    {
        public List<Rotator> Levels { get; set; }
        private const int NumberOfLevels = 10;
        private int CurrentLevel;

        public GameLevels()
        {
            Levels = new List<Rotator>();
            CurrentLevel = 0;
            InitLevels();
        }

        private void InitLevels()
        {
            for (int i = 0; i < NumberOfLevels; i++)
            {
                Rotator level = new Rotator((i + 15.0) / NumberOfLevels);
                level.NumberOfBallsToShoot = (int)(((double)(i * 11) / NumberOfLevels) + 5);
                level.NumberOfBallsRotator = (int)(((double)(i * 16) / NumberOfLevels) + 2);

                if (i % 3 == 0)
                {
                    //CheckCollision doesn't work for CounterClockWise rotator
                    //level.RotateClockWise = false;
                }
                else
                {
                    level.RotateClockWise = true;
                }

                if (i % 5 == 0)
                {
                    //level.Speed = 2;
                }

                if (i == 8)
                {
                    level.RotateClockWise = false;
                }

                // should be in Rotator class
                for (int j = 0; j < level.NumberOfBallsRotator; j++)
                {
                    level.Balls.Add(new Ball(j * (360 / level.NumberOfBallsRotator)));
                }

                Levels.Add(level);
            }
        }

        public Rotator NextLevel()
        {
            if (CurrentLevel < NumberOfLevels)
            {
                int i = CurrentLevel;
                CurrentLevel++;
                return Levels[i];
            }

            return null;
        }
    }
}