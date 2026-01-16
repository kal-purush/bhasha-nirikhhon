using Data;
using UnityEngine;

namespace UI
{
    public static class UICheckShapePosition
    {
        private static readonly float Allowance = 0.3f;
        private static readonly float AllowanceLong = 1f;
        public static bool IsPositionBlockRight;

        public static bool CheckPosition(int levelNumber, LevelInfo levelInfo)
        {
            if (levelNumber == 1)
            {
                float[] values = DataGame.CheckDistance(levelNumber);
                if (Vector3.Distance(levelInfo.blockOne.transform.position, levelInfo.blockTwo.transform.position) < values[0] + Allowance
                    && Vector3.Distance(levelInfo.blockOne.transform.position, levelInfo.blockTwo.transform.position) > values[0] - Allowance && 
                    Vector3.Distance(levelInfo.blockTwo.transform.position, levelInfo.blockFour.transform.position) < values[1] + Allowance
                    && Vector3.Distance(levelInfo.blockTwo.transform.position, levelInfo.blockFour.transform.position) > values[1] - Allowance &&
                    Vector3.Distance(levelInfo.blockOne.transform.position, levelInfo.blockFour.transform.position) < values[2] + Allowance
                    && Vector3.Distance(levelInfo.blockOne.transform.position, levelInfo.blockFour.transform.position) > values[2] - Allowance &&
                    Vector3.Distance(levelInfo.blockThree.transform.position, levelInfo.blockFour.transform.position) < values[3] + AllowanceLong
                    && Vector3.Distance(levelInfo.blockThree.transform.position, levelInfo.blockFour.transform.position) > values[3] - AllowanceLong &&
                    Vector3.Distance(levelInfo.blockFour.transform.position, levelInfo.blockFive.transform.position) < values[4] + Allowance
                    && Vector3.Distance(levelInfo.blockFour.transform.position, levelInfo.blockFive.transform.position) > values[4] - Allowance)
                {
                    IsPositionBlockRight = true;
                }
                if (Vector3.Distance(levelInfo.blockOne.transform.position, levelInfo.blockTwo.transform.position) < values[0] + Allowance
                    && Vector3.Distance(levelInfo.blockOne.transform.position, levelInfo.blockTwo.transform.position) > values[0] - Allowance && 
                    Vector3.Distance(levelInfo.blockOne.transform.position, levelInfo.blockFour.transform.position) < values[1] + Allowance
                    && Vector3.Distance(levelInfo.blockOne.transform.position, levelInfo.blockFour.transform.position) > values[1] - Allowance &&
                    Vector3.Distance(levelInfo.blockTwo.transform.position, levelInfo.blockFour.transform.position) < values[2] + Allowance
                    && Vector3.Distance(levelInfo.blockTwo.transform.position, levelInfo.blockFour.transform.position) > values[2] - Allowance &&
                    Vector3.Distance(levelInfo.blockThree.transform.position, levelInfo.blockFour.transform.position) < values[3] + AllowanceLong
                    && Vector3.Distance(levelInfo.blockThree.transform.position, levelInfo.blockFour.transform.position) > values[3] - AllowanceLong &&
                    Vector3.Distance(levelInfo.blockFour.transform.position, levelInfo.blockFive.transform.position) < values[4] + Allowance 
                    && Vector3.Distance(levelInfo.blockFour.transform.position, levelInfo.blockFive.transform.position) > values[4] - Allowance)
                {
                    IsPositionBlockRight = true;
                }
                
                if (Vector3.Distance(levelInfo.blockOne.transform.position, levelInfo.blockTwo.transform.position) < values[0] + Allowance
                    && Vector3.Distance(levelInfo.blockOne.transform.position, levelInfo.blockTwo.transform.position) > values[0] - Allowance && 
                    Vector3.Distance(levelInfo.blockTwo.transform.position, levelInfo.blockFour.transform.position) < values[1] + Allowance
                    && Vector3.Distance(levelInfo.blockTwo.transform.position, levelInfo.blockFour.transform.position) > values[1] - Allowance &&
                    Vector3.Distance(levelInfo.blockOne.transform.position, levelInfo.blockFour.transform.position) < values[2] + Allowance
                    && Vector3.Distance(levelInfo.blockOne.transform.position, levelInfo.blockFour.transform.position) > values[2] - Allowance &&
                    Vector3.Distance(levelInfo.blockThree.transform.position, levelInfo.blockFour.transform.position) < values[5] + AllowanceLong
                    && Vector3.Distance(levelInfo.blockThree.transform.position, levelInfo.blockFour.transform.position) > values[5] - AllowanceLong &&
                    Vector3.Distance(levelInfo.blockFour.transform.position, levelInfo.blockFive.transform.position) < values[4] + Allowance
                    && Vector3.Distance(levelInfo.blockFour.transform.position, levelInfo.blockFive.transform.position) > values[4] - Allowance)
                {
                    IsPositionBlockRight = true;
                }
                if (Vector3.Distance(levelInfo.blockOne.transform.position, levelInfo.blockTwo.transform.position) < values[0] + Allowance
                    && Vector3.Distance(levelInfo.blockOne.transform.position, levelInfo.blockTwo.transform.position) > values[0] - Allowance && 
                    Vector3.Distance(levelInfo.blockOne.transform.position, levelInfo.blockFour.transform.position) < values[1] + Allowance
                    && Vector3.Distance(levelInfo.blockOne.transform.position, levelInfo.blockFour.transform.position) > values[1] - Allowance&&
                    Vector3.Distance(levelInfo.blockTwo.transform.position, levelInfo.blockFour.transform.position) < values[2] + Allowance
                    && Vector3.Distance(levelInfo.blockTwo.transform.position, levelInfo.blockFour.transform.position) > values[2] - Allowance &&
                    Vector3.Distance(levelInfo.blockThree.transform.position, levelInfo.blockFour.transform.position) < values[5] + AllowanceLong
                    && Vector3.Distance(levelInfo.blockThree.transform.position, levelInfo.blockFour.transform.position) > values[5] - AllowanceLong &&
                    Vector3.Distance(levelInfo.blockFour.transform.position, levelInfo.blockFive.transform.position) < values[4] + Allowance
                    && Vector3.Distance(levelInfo.blockFour.transform.position, levelInfo.blockFive.transform.position) > values[4] - Allowance)
                {
                    IsPositionBlockRight = true;
                }
            }

            if (levelNumber == 2)
            {
                float[] values = DataGame.CheckDistance(levelNumber);
                if (Vector3.Distance(levelInfo.blockOne.transform.position, levelInfo.blockTwo.transform.position) < values[0] + Allowance
                    && Vector3.Distance(levelInfo.blockOne.transform.position, levelInfo.blockTwo.transform.position) > values[0] - Allowance && 
                    Vector3.Distance(levelInfo.blockOne.transform.position, levelInfo.blockFive.transform.position) < values[1] + Allowance
                    && Vector3.Distance(levelInfo.blockOne.transform.position, levelInfo.blockFive.transform.position) > values[1] - Allowance &&
                    Vector3.Distance(levelInfo.blockTwo.transform.position, levelInfo.blockFour.transform.position) < values[2] + Allowance
                    && Vector3.Distance(levelInfo.blockTwo.transform.position, levelInfo.blockFour.transform.position) > values[2] - Allowance &&
                    Vector3.Distance(levelInfo.blockThree.transform.position, levelInfo.blockFive.transform.position) < values[3] + AllowanceLong
                    && Vector3.Distance(levelInfo.blockThree.transform.position, levelInfo.blockFive.transform.position) > values[3] - AllowanceLong &&
                    Vector3.Distance(levelInfo.blockFour.transform.position, levelInfo.blockFive.transform.position) < values[4] + Allowance
                    && Vector3.Distance(levelInfo.blockFour.transform.position, levelInfo.blockFive.transform.position) > values[4] - Allowance)
                {
                    IsPositionBlockRight = true;
                }
            }
            
            if (levelNumber == 3)
            {
                float[] values = DataGame.CheckDistance(levelNumber);
                if (Vector3.Distance(levelInfo.blockOne.transform.position, levelInfo.blockTwo.transform.position) < values[0] + Allowance
                    && Vector3.Distance(levelInfo.blockOne.transform.position, levelInfo.blockTwo.transform.position) > values[0] - Allowance &&
                    Vector3.Distance(levelInfo.blockTwo.transform.position, levelInfo.blockThree.transform.position) < values[1] + Allowance
                    && Vector3.Distance(levelInfo.blockTwo.transform.position, levelInfo.blockThree.transform.position) > values[1] - Allowance &&
                    Vector3.Distance(levelInfo.blockThree.transform.position, levelInfo.blockFour.transform.position) < values[2] + Allowance
                    && Vector3.Distance(levelInfo.blockThree.transform.position, levelInfo.blockFour.transform.position) > values[2] - Allowance &&
                    Vector3.Distance(levelInfo.blockFour.transform.position, levelInfo.blockOne.transform.position) < values[3] + Allowance
                    && Vector3.Distance(levelInfo.blockFour.transform.position, levelInfo.blockOne.transform.position) > values[3] - Allowance)
                {
                    IsPositionBlockRight = true;
                }
                if (Vector3.Distance(levelInfo.blockOne.transform.position, levelInfo.blockThree.transform.position) < values[0] + Allowance
                    && Vector3.Distance(levelInfo.blockOne.transform.position, levelInfo.blockThree.transform.position) > values[0] - Allowance &&
                    Vector3.Distance(levelInfo.blockTwo.transform.position, levelInfo.blockThree.transform.position) < values[1] + Allowance
                    && Vector3.Distance(levelInfo.blockTwo.transform.position, levelInfo.blockThree.transform.position) > values[1] - Allowance &&
                    Vector3.Distance(levelInfo.blockTwo.transform.position, levelInfo.blockFour.transform.position) < values[2] + Allowance
                    && Vector3.Distance(levelInfo.blockTwo.transform.position, levelInfo.blockFour.transform.position) > values[2] - Allowance &&
                    Vector3.Distance(levelInfo.blockFour.transform.position, levelInfo.blockOne.transform.position) < values[3] + Allowance
                    && Vector3.Distance(levelInfo.blockFour.transform.position, levelInfo.blockOne.transform.position) > values[3] - Allowance)
                {
                    IsPositionBlockRight = true;
                }
                if (Vector3.Distance(levelInfo.blockOne.transform.position, levelInfo.blockThree.transform.position) < values[0] + Allowance
                    && Vector3.Distance(levelInfo.blockOne.transform.position, levelInfo.blockThree.transform.position) > values[0] - Allowance &&
                    Vector3.Distance(levelInfo.blockThree.transform.position, levelInfo.blockFour.transform.position) < values[1] + Allowance
                    && Vector3.Distance(levelInfo.blockThree.transform.position, levelInfo.blockFour.transform.position) > values[1] - Allowance &&
                    Vector3.Distance(levelInfo.blockFour.transform.position, levelInfo.blockTwo.transform.position) < values[2] + Allowance
                    && Vector3.Distance(levelInfo.blockFour.transform.position, levelInfo.blockTwo.transform.position) > values[2] - Allowance &&
                    Vector3.Distance(levelInfo.blockTwo.transform.position, levelInfo.blockOne.transform.position) < values[3] + Allowance
                    && Vector3.Distance(levelInfo.blockTwo.transform.position, levelInfo.blockOne.transform.position) > values[3] - Allowance)
                {
                    IsPositionBlockRight = true;
                }
            }

            if (levelNumber == 4)
            {
                float[] values = DataGame.CheckDistance(levelNumber);
                if (Vector3.Distance(levelInfo.blockOne.transform.position, levelInfo.blockFour.transform.position) < values[0] + Allowance
                    && Vector3.Distance(levelInfo.blockOne.transform.position, levelInfo.blockFour.transform.position) > values[0] - Allowance &&
                    Vector3.Distance(levelInfo.blockFour.transform.position, levelInfo.blockTwo.transform.position) < values[1] + Allowance
                    && Vector3.Distance(levelInfo.blockFour.transform.position, levelInfo.blockTwo.transform.position) > values[1] - Allowance &&
                    Vector3.Distance(levelInfo.blockFour.transform.position, levelInfo.blockFive.transform.position) < values[2] + Allowance
                    && Vector3.Distance(levelInfo.blockFour.transform.position, levelInfo.blockFive.transform.position) > values[2] - Allowance &&
                    Vector3.Distance(levelInfo.blockThree.transform.position, levelInfo.blockFour.transform.position) < values[3] + Allowance
                    && Vector3.Distance(levelInfo.blockThree.transform.position, levelInfo.blockFour.transform.position) > values[3] - Allowance)
                {
                    IsPositionBlockRight = true;
                }
            }
            
            if (levelNumber == 5)
            {
                float[] values = DataGame.CheckDistance(levelNumber);
                if (Vector3.Distance(levelInfo.blockOne.transform.position, levelInfo.blockFive.transform.position) < values[0] + Allowance
                    && Vector3.Distance(levelInfo.blockOne.transform.position, levelInfo.blockFive.transform.position) > values[0] - Allowance &&
                    Vector3.Distance(levelInfo.blockTwo.transform.position, levelInfo.blockFive.transform.position) < values[1] + Allowance
                    && Vector3.Distance(levelInfo.blockTwo.transform.position, levelInfo.blockFive.transform.position) > values[1] - Allowance &&
                    Vector3.Distance(levelInfo.blockThree.transform.position, levelInfo.blockFive.transform.position) < values[2] + Allowance
                    && Vector3.Distance(levelInfo.blockThree.transform.position, levelInfo.blockFive.transform.position) > values[2] - Allowance &&
                    Vector3.Distance(levelInfo.blockFour.transform.position, levelInfo.blockFive.transform.position) < values[3] + Allowance
                    && Vector3.Distance(levelInfo.blockFour.transform.position, levelInfo.blockFive.transform.position) > values[3] - Allowance)
                {
                    IsPositionBlockRight = true;
                }
            }

            if (levelNumber == 6)
            {
                float[] values = DataGame.CheckDistance(levelNumber);
                if (Vector3.Distance(levelInfo.blockOne.transform.position, levelInfo.blockFive.transform.position) < values[0] + Allowance
                    && Vector3.Distance(levelInfo.blockOne.transform.position, levelInfo.blockFive.transform.position) > values[0] - Allowance &&
                    Vector3.Distance(levelInfo.blockTwo.transform.position, levelInfo.blockFive.transform.position) < values[1] + Allowance
                    && Vector3.Distance(levelInfo.blockTwo.transform.position, levelInfo.blockFive.transform.position) > values[1] - Allowance &&
                    Vector3.Distance(levelInfo.blockThree.transform.position, levelInfo.blockFour.transform.position) < values[2] + Allowance
                    && Vector3.Distance(levelInfo.blockThree.transform.position, levelInfo.blockFour.transform.position) > values[2] - Allowance &&
                    Vector3.Distance(levelInfo.blockThree.transform.position, levelInfo.blockFive.transform.position) < values[3] + Allowance
                    && Vector3.Distance(levelInfo.blockThree.transform.position, levelInfo.blockFive.transform.position) > values[3] - Allowance &&
                    Vector3.Distance(levelInfo.blockFour.transform.position, levelInfo.blockFive.transform.position) < values[4] + Allowance
                    && Vector3.Distance(levelInfo.blockFour.transform.position, levelInfo.blockFive.transform.position) > values[4] - Allowance)
                {
                    IsPositionBlockRight = true;
                }
                
                if (Vector3.Distance(levelInfo.blockTwo.transform.position, levelInfo.blockFive.transform.position) < values[0] + Allowance
                    && Vector3.Distance(levelInfo.blockTwo.transform.position, levelInfo.blockFive.transform.position) > values[0] - Allowance &&
                    Vector3.Distance(levelInfo.blockOne.transform.position, levelInfo.blockFive.transform.position) < values[1] + Allowance
                    && Vector3.Distance(levelInfo.blockOne.transform.position, levelInfo.blockFive.transform.position) > values[1] - Allowance &&
                    Vector3.Distance(levelInfo.blockThree.transform.position, levelInfo.blockFour.transform.position) < values[2] + Allowance
                    && Vector3.Distance(levelInfo.blockThree.transform.position, levelInfo.blockFour.transform.position) > values[2] - Allowance &&
                    Vector3.Distance(levelInfo.blockThree.transform.position, levelInfo.blockFive.transform.position) < values[3] + Allowance
                    && Vector3.Distance(levelInfo.blockThree.transform.position, levelInfo.blockFive.transform.position) > values[3] - Allowance &&
                    Vector3.Distance(levelInfo.blockFour.transform.position, levelInfo.blockFive.transform.position) < values[4] + Allowance
                    && Vector3.Distance(levelInfo.blockFour.transform.position, levelInfo.blockFive.transform.position) > values[4] - Allowance)
                {
                    IsPositionBlockRight = true;
                }
            }
            
            if (levelNumber == 7)
            {
                float[] values = DataGame.CheckDistance(levelNumber);
                if (Vector3.Distance(levelInfo.blockOne.transform.position, levelInfo.blockTwo.transform.position) < values[0] + Allowance
                    && Vector3.Distance(levelInfo.blockOne.transform.position, levelInfo.blockTwo.transform.position) > values[0] - Allowance &&
                    Vector3.Distance(levelInfo.blockThree.transform.position, levelInfo.blockFour.transform.position) < values[1] + Allowance
                    && Vector3.Distance(levelInfo.blockThree.transform.position, levelInfo.blockFour.transform.position) > values[1] - Allowance &&
                    Vector3.Distance(levelInfo.blockFour.transform.position, levelInfo.blockFive.transform.position) < values[2] + Allowance
                    && Vector3.Distance(levelInfo.blockFour.transform.position, levelInfo.blockFive.transform.position) > values[2] - Allowance &&
                    Vector3.Distance(levelInfo.blockTwo.transform.position, levelInfo.blockFour.transform.position) < values[3] + Allowance
                    && Vector3.Distance(levelInfo.blockTwo.transform.position, levelInfo.blockFour.transform.position) > values[3] - Allowance)
                {
                    IsPositionBlockRight = true;
                }
                
                if (Vector3.Distance(levelInfo.blockOne.transform.position, levelInfo.blockTwo.transform.position) < values[0] + Allowance
                    && Vector3.Distance(levelInfo.blockOne.transform.position, levelInfo.blockTwo.transform.position) > values[0] - Allowance &&
                    Vector3.Distance(levelInfo.blockThree.transform.position, levelInfo.blockFour.transform.position) < values[1] + Allowance
                    && Vector3.Distance(levelInfo.blockThree.transform.position, levelInfo.blockFour.transform.position) > values[1] - Allowance &&
                    Vector3.Distance(levelInfo.blockFour.transform.position, levelInfo.blockFive.transform.position) < values[2] + Allowance
                    && Vector3.Distance(levelInfo.blockFour.transform.position, levelInfo.blockFive.transform.position) > values[2] - Allowance &&
                    Vector3.Distance(levelInfo.blockOne.transform.position, levelInfo.blockFour.transform.position) < values[3] + Allowance
                    && Vector3.Distance(levelInfo.blockOne.transform.position, levelInfo.blockFour.transform.position) > values[3] - Allowance)
                {
                    IsPositionBlockRight = true;
                }
            }
            
            if (levelNumber == 8)
            {
                float[] values = DataGame.CheckDistance(levelNumber);
                if (Vector3.Distance(levelInfo.blockFour.transform.position, levelInfo.blockFive.transform.position) < values[0] + Allowance
                    && Vector3.Distance(levelInfo.blockFour.transform.position, levelInfo.blockFive.transform.position) > values[0] - Allowance &&
                    Vector3.Distance(levelInfo.blockOne.transform.position, levelInfo.blockFour.transform.position) < values[1] + Allowance
                    && Vector3.Distance(levelInfo.blockOne.transform.position, levelInfo.blockFour.transform.position) > values[1] - Allowance &&
                    Vector3.Distance(levelInfo.blockTwo.transform.position, levelInfo.blockFour.transform.position) < values[2] + Allowance
                    && Vector3.Distance(levelInfo.blockTwo.transform.position, levelInfo.blockFour.transform.position) > values[2] - Allowance &&
                    Vector3.Distance(levelInfo.blockThree.transform.position, levelInfo.blockFive.transform.position) < values[3] + Allowance
                    && Vector3.Distance(levelInfo.blockThree.transform.position, levelInfo.blockFive.transform.position) > values[3] - Allowance)
                {
                    IsPositionBlockRight = true;
                }
                if (Vector3.Distance(levelInfo.blockFour.transform.position, levelInfo.blockFive.transform.position) < values[0] + Allowance
                    && Vector3.Distance(levelInfo.blockFour.transform.position, levelInfo.blockFive.transform.position) > values[0] - Allowance &&
                    Vector3.Distance(levelInfo.blockOne.transform.position, levelInfo.blockFour.transform.position) < values[1] + Allowance
                    && Vector3.Distance(levelInfo.blockOne.transform.position, levelInfo.blockFour.transform.position) > values[1] - Allowance &&
                    Vector3.Distance(levelInfo.blockThree.transform.position, levelInfo.blockFour.transform.position) < values[2] + Allowance
                    && Vector3.Distance(levelInfo.blockThree.transform.position, levelInfo.blockFour.transform.position) > values[2] - Allowance &&
                    Vector3.Distance(levelInfo.blockTwo.transform.position, levelInfo.blockFive.transform.position) < values[3] + Allowance
                    && Vector3.Distance(levelInfo.blockTwo.transform.position, levelInfo.blockFive.transform.position) > values[3] - Allowance)
                {
                    IsPositionBlockRight = true;
                }
                if (Vector3.Distance(levelInfo.blockFour.transform.position, levelInfo.blockFive.transform.position) < values[0] + Allowance
                    && Vector3.Distance(levelInfo.blockFour.transform.position, levelInfo.blockFive.transform.position) > values[0] - Allowance &&
                    Vector3.Distance(levelInfo.blockTwo.transform.position, levelInfo.blockFour.transform.position) < values[1] + Allowance
                    && Vector3.Distance(levelInfo.blockTwo.transform.position, levelInfo.blockFour.transform.position) > values[1] - Allowance &&
                    Vector3.Distance(levelInfo.blockThree.transform.position, levelInfo.blockFour.transform.position) < values[2] + Allowance
                    && Vector3.Distance(levelInfo.blockThree.transform.position, levelInfo.blockFour.transform.position) > values[2] - Allowance &&
                    Vector3.Distance(levelInfo.blockOne.transform.position, levelInfo.blockFive.transform.position) < values[3] + Allowance
                    && Vector3.Distance(levelInfo.blockOne.transform.position, levelInfo.blockFive.transform.position) > values[3] - Allowance)
                {
                    IsPositionBlockRight = true;
                }
                if (Vector3.Distance(levelInfo.blockFour.transform.position, levelInfo.blockFive.transform.position) < values[0] + Allowance
                    && Vector3.Distance(levelInfo.blockFour.transform.position, levelInfo.blockFive.transform.position) > values[0] - Allowance &&
                    Vector3.Distance(levelInfo.blockTwo.transform.position, levelInfo.blockFour.transform.position) < values[1] + Allowance
                    && Vector3.Distance(levelInfo.blockTwo.transform.position, levelInfo.blockFour.transform.position) > values[1] - Allowance &&
                    Vector3.Distance(levelInfo.blockOne.transform.position, levelInfo.blockFour.transform.position) < values[2] + Allowance
                    && Vector3.Distance(levelInfo.blockOne.transform.position, levelInfo.blockFour.transform.position) > values[2] - Allowance &&
                    Vector3.Distance(levelInfo.blockThree.transform.position, levelInfo.blockFive.transform.position) < values[3] + Allowance
                    && Vector3.Distance(levelInfo.blockOne.transform.position, levelInfo.blockFive.transform.position) > values[3] - Allowance)
                {
                    IsPositionBlockRight = true;
                }
                if (Vector3.Distance(levelInfo.blockFour.transform.position, levelInfo.blockFive.transform.position) < values[0] + Allowance
                    && Vector3.Distance(levelInfo.blockFour.transform.position, levelInfo.blockFive.transform.position) > values[0] - Allowance &&
                    Vector3.Distance(levelInfo.blockThree.transform.position, levelInfo.blockFour.transform.position) < values[1] + Allowance
                    && Vector3.Distance(levelInfo.blockThree.transform.position, levelInfo.blockFour.transform.position) > values[1] - Allowance &&
                    Vector3.Distance(levelInfo.blockTwo.transform.position, levelInfo.blockFour.transform.position) < values[2] + Allowance
                    && Vector3.Distance(levelInfo.blockTwo.transform.position, levelInfo.blockFour.transform.position) > values[2] - Allowance &&
                    Vector3.Distance(levelInfo.blockOne.transform.position, levelInfo.blockFive.transform.position) < values[3] + Allowance
                    && Vector3.Distance(levelInfo.blockOne.transform.position, levelInfo.blockFive.transform.position) > values[3] - Allowance)
                {
                    IsPositionBlockRight = true;
                }
                if (Vector3.Distance(levelInfo.blockFour.transform.position, levelInfo.blockFive.transform.position) < values[0] + Allowance
                    && Vector3.Distance(levelInfo.blockFour.transform.position, levelInfo.blockFive.transform.position) > values[0] - Allowance &&
                    Vector3.Distance(levelInfo.blockThree.transform.position, levelInfo.blockFour.transform.position) < values[1] + Allowance
                    && Vector3.Distance(levelInfo.blockThree.transform.position, levelInfo.blockFour.transform.position) > values[1] - Allowance &&
                    Vector3.Distance(levelInfo.blockOne.transform.position, levelInfo.blockFour.transform.position) < values[2] + Allowance
                    && Vector3.Distance(levelInfo.blockOne.transform.position, levelInfo.blockFour.transform.position) > values[2] - Allowance &&
                    Vector3.Distance(levelInfo.blockTwo.transform.position, levelInfo.blockFive.transform.position) < values[3] + Allowance
                    && Vector3.Distance(levelInfo.blockTwo.transform.position, levelInfo.blockFive.transform.position) > values[3] - Allowance)
                {
                    IsPositionBlockRight = true;
                }
            }
            
            if (levelNumber == 9)
            {
                float[] values = DataGame.CheckDistance(levelNumber);
                if (Vector3.Distance(levelInfo.blockOne.transform.position, levelInfo.blockTwo.transform.position) < values[0] + Allowance
                    && Vector3.Distance(levelInfo.blockOne.transform.position, levelInfo.blockTwo.transform.position) > values[0] - Allowance &&
                    Vector3.Distance(levelInfo.blockOne.transform.position, levelInfo.blockThree.transform.position) < values[1] + Allowance
                    && Vector3.Distance(levelInfo.blockOne.transform.position, levelInfo.blockThree.transform.position) > values[1] - Allowance &&
                    Vector3.Distance(levelInfo.blockThree.transform.position, levelInfo.blockFive.transform.position) < values[2] + Allowance
                    && Vector3.Distance(levelInfo.blockThree.transform.position, levelInfo.blockFive.transform.position) > values[2] -Allowance &&
                    Vector3.Distance(levelInfo.blockOne.transform.position, levelInfo.blockFour.transform.position) < values[3] + Allowance
                    && Vector3.Distance(levelInfo.blockOne.transform.position, levelInfo.blockFour.transform.position) > values[3] -Allowance)
                {
                    IsPositionBlockRight = true;
                }
            }
            
            if (levelNumber == 10)
            {
                float[] values = DataGame.CheckDistance(levelNumber);
                if (Vector3.Distance(levelInfo.blockOne.transform.position, levelInfo.blockFive.transform.position) < values[0] + Allowance
                    && Vector3.Distance(levelInfo.blockOne.transform.position, levelInfo.blockFive.transform.position) > values[0] - Allowance &&
                    Vector3.Distance(levelInfo.blockOne.transform.position, levelInfo.blockTwo.transform.position) < values[1] + Allowance
                    && Vector3.Distance(levelInfo.blockOne.transform.position, levelInfo.blockTwo.transform.position) > values[1] - Allowance &&
                    Vector3.Distance(levelInfo.blockOne.transform.position, levelInfo.blockThree.transform.position) < values[2] + Allowance
                    && Vector3.Distance(levelInfo.blockOne.transform.position, levelInfo.blockThree.transform.position) > values[2] -Allowance &&
                    Vector3.Distance(levelInfo.blockOne.transform.position, levelInfo.blockFour.transform.position) < values[3] + Allowance
                    && Vector3.Distance(levelInfo.blockOne.transform.position, levelInfo.blockFour.transform.position) > values[3] -Allowance)
                {
                    IsPositionBlockRight = true;
                }
                
                if (Vector3.Distance(levelInfo.blockOne.transform.position, levelInfo.blockFive.transform.position) < values[0] + Allowance
                    && Vector3.Distance(levelInfo.blockOne.transform.position, levelInfo.blockFive.transform.position) > values[0] - Allowance &&
                    Vector3.Distance(levelInfo.blockOne.transform.position, levelInfo.blockTwo.transform.position) < values[1] + Allowance
                    && Vector3.Distance(levelInfo.blockOne.transform.position, levelInfo.blockTwo.transform.position) > values[1] - Allowance &&
                    Vector3.Distance(levelInfo.blockOne.transform.position, levelInfo.blockFour.transform.position) < values[2] + Allowance
                    && Vector3.Distance(levelInfo.blockOne.transform.position, levelInfo.blockFour.transform.position) > values[2] -Allowance &&
                    Vector3.Distance(levelInfo.blockOne.transform.position, levelInfo.blockThree.transform.position) < values[3] + Allowance
                    && Vector3.Distance(levelInfo.blockOne.transform.position, levelInfo.blockThree.transform.position) > values[3] -Allowance)
                {
                    IsPositionBlockRight = true;
                }
                
                if (Vector3.Distance(levelInfo.blockOne.transform.position, levelInfo.blockFive.transform.position) < values[0] + Allowance
                    && Vector3.Distance(levelInfo.blockOne.transform.position, levelInfo.blockFive.transform.position) > values[0] - Allowance &&
                    Vector3.Distance(levelInfo.blockOne.transform.position, levelInfo.blockThree.transform.position) < values[1] + Allowance
                    && Vector3.Distance(levelInfo.blockOne.transform.position, levelInfo.blockThree.transform.position) > values[1] - Allowance &&
                    Vector3.Distance(levelInfo.blockOne.transform.position, levelInfo.blockTwo.transform.position) < values[2] + Allowance
                    && Vector3.Distance(levelInfo.blockOne.transform.position, levelInfo.blockTwo.transform.position) > values[2] -Allowance &&
                    Vector3.Distance(levelInfo.blockOne.transform.position, levelInfo.blockFour.transform.position) < values[3] + Allowance
                    && Vector3.Distance(levelInfo.blockOne.transform.position, levelInfo.blockFour.transform.position) > values[3] -Allowance)
                {
                    IsPositionBlockRight = true;
                }
                
                if (Vector3.Distance(levelInfo.blockOne.transform.position, levelInfo.blockFive.transform.position) < values[0] + Allowance
                    && Vector3.Distance(levelInfo.blockOne.transform.position, levelInfo.blockFive.transform.position) > values[0] - Allowance &&
                    Vector3.Distance(levelInfo.blockOne.transform.position, levelInfo.blockThree.transform.position) < values[1] + Allowance
                    && Vector3.Distance(levelInfo.blockOne.transform.position, levelInfo.blockThree.transform.position) > values[1] - Allowance &&
                    Vector3.Distance(levelInfo.blockOne.transform.position, levelInfo.blockFour.transform.position) < values[2] + Allowance
                    && Vector3.Distance(levelInfo.blockOne.transform.position, levelInfo.blockFour.transform.position) > values[2] -Allowance &&
                    Vector3.Distance(levelInfo.blockOne.transform.position, levelInfo.blockTwo.transform.position) < values[3] + Allowance
                    && Vector3.Distance(levelInfo.blockOne.transform.position, levelInfo.blockTwo.transform.position) > values[3] -Allowance)
                {
                    IsPositionBlockRight = true;
                }
                
                if (Vector3.Distance(levelInfo.blockOne.transform.position, levelInfo.blockFive.transform.position) < values[0] + Allowance
                    && Vector3.Distance(levelInfo.blockOne.transform.position, levelInfo.blockFive.transform.position) > values[0] - Allowance &&
                    Vector3.Distance(levelInfo.blockOne.transform.position, levelInfo.blockFour.transform.position) < values[1] + Allowance
                    && Vector3.Distance(levelInfo.blockOne.transform.position, levelInfo.blockFour.transform.position) > values[1] - Allowance &&
                    Vector3.Distance(levelInfo.blockOne.transform.position, levelInfo.blockThree.transform.position) < values[2] + Allowance
                    && Vector3.Distance(levelInfo.blockOne.transform.position, levelInfo.blockThree.transform.position) > values[2] -Allowance &&
                    Vector3.Distance(levelInfo.blockOne.transform.position, levelInfo.blockTwo.transform.position) < values[3] + Allowance
                    && Vector3.Distance(levelInfo.blockOne.transform.position, levelInfo.blockTwo.transform.position) > values[3] -Allowance)
                {
                    IsPositionBlockRight = true;
                }
                
                if (Vector3.Distance(levelInfo.blockOne.transform.position, levelInfo.blockFive.transform.position) < values[0] + Allowance
                    && Vector3.Distance(levelInfo.blockOne.transform.position, levelInfo.blockFive.transform.position) > values[0] - Allowance &&
                    Vector3.Distance(levelInfo.blockOne.transform.position, levelInfo.blockFour.transform.position) < values[1] + Allowance
                    && Vector3.Distance(levelInfo.blockOne.transform.position, levelInfo.blockFour.transform.position) > values[1] - Allowance &&
                    Vector3.Distance(levelInfo.blockOne.transform.position, levelInfo.blockTwo.transform.position) < values[2] + Allowance
                    && Vector3.Distance(levelInfo.blockOne.transform.position, levelInfo.blockTwo.transform.position) > values[2] -Allowance &&
                    Vector3.Distance(levelInfo.blockOne.transform.position, levelInfo.blockThree.transform.position) < values[3] + Allowance
                    && Vector3.Distance(levelInfo.blockOne.transform.position, levelInfo.blockThree.transform.position) > values[3] -Allowance)
                {
                    IsPositionBlockRight = true;
                }
            }
            
            if (levelNumber == 11)
            {
                float[] values = DataGame.CheckDistance(levelNumber);
                if (Vector3.Distance(levelInfo.blockOne.transform.position, levelInfo.blockTwo.transform.position) < values[0] + Allowance
                    && Vector3.Distance(levelInfo.blockOne.transform.position, levelInfo.blockTwo.transform.position) > values[0] - Allowance &&
                    Vector3.Distance(levelInfo.blockTwo.transform.position, levelInfo.blockThree.transform.position) < values[1] + Allowance
                    && Vector3.Distance(levelInfo.blockTwo.transform.position, levelInfo.blockThree.transform.position) > values[1] - Allowance &&
                    Vector3.Distance(levelInfo.blockThree.transform.position, levelInfo.blockFive.transform.position) < values[2] + Allowance
                    && Vector3.Distance(levelInfo.blockThree.transform.position, levelInfo.blockFive.transform.position) > values[2] -Allowance &&
                    Vector3.Distance(levelInfo.blockTwo.transform.position, levelInfo.blockFour.transform.position) < values[3] + Allowance
                    && Vector3.Distance(levelInfo.blockTwo.transform.position, levelInfo.blockFour.transform.position) > values[3] -Allowance)
                {
                    IsPositionBlockRight = true;
                }
            }
            
            if (levelNumber == 12)
            {
                float[] values = DataGame.CheckDistance(levelNumber);
                if (Vector3.Distance(levelInfo.blockOne.transform.position, levelInfo.blockTwo.transform.position) < values[0] + Allowance
                    && Vector3.Distance(levelInfo.blockOne.transform.position, levelInfo.blockTwo.transform.position) > values[0] - Allowance &&
                    Vector3.Distance(levelInfo.blockTwo.transform.position, levelInfo.blockThree.transform.position) < values[1] + Allowance
                    && Vector3.Distance(levelInfo.blockTwo.transform.position, levelInfo.blockThree.transform.position) > values[1] - Allowance &&
                    Vector3.Distance(levelInfo.blockFour.transform.position, levelInfo.blockFive.transform.position) < values[2] + Allowance
                    && Vector3.Distance(levelInfo.blockFour.transform.position, levelInfo.blockFive.transform.position) > values[2] -Allowance &&
                    Vector3.Distance(levelInfo.blockOne.transform.position, levelInfo.blockFive.transform.position) < values[3] + Allowance
                    && Vector3.Distance(levelInfo.blockOne.transform.position, levelInfo.blockFive.transform.position) > values[3] -Allowance)
                {
                    IsPositionBlockRight = true;
                }
                if (Vector3.Distance(levelInfo.blockOne.transform.position, levelInfo.blockTwo.transform.position) < values[0] + Allowance
                    && Vector3.Distance(levelInfo.blockOne.transform.position, levelInfo.blockTwo.transform.position) > values[0] - Allowance &&
                    Vector3.Distance(levelInfo.blockTwo.transform.position, levelInfo.blockThree.transform.position) < values[1] + Allowance
                    && Vector3.Distance(levelInfo.blockTwo.transform.position, levelInfo.blockThree.transform.position) > values[1] - Allowance &&
                    Vector3.Distance(levelInfo.blockFour.transform.position, levelInfo.blockFive.transform.position) < values[2] + Allowance
                    && Vector3.Distance(levelInfo.blockFour.transform.position, levelInfo.blockFive.transform.position) > values[2] -Allowance &&
                    Vector3.Distance(levelInfo.blockOne.transform.position, levelInfo.blockFour.transform.position) < values[3] + Allowance
                    && Vector3.Distance(levelInfo.blockOne.transform.position, levelInfo.blockFour.transform.position) > values[3] -Allowance)
                {
                    IsPositionBlockRight = true;
                }
            }
            
            if (levelNumber == 13)
            {
                float[] values = DataGame.CheckDistance(levelNumber);
                if (Vector3.Distance(levelInfo.blockOne.transform.position, levelInfo.blockTwo.transform.position) < values[0] + Allowance
                    && Vector3.Distance(levelInfo.blockOne.transform.position, levelInfo.blockTwo.transform.position) > values[0] - Allowance &&
                    Vector3.Distance(levelInfo.blockOne.transform.position, levelInfo.blockThree.transform.position) < values[1] + Allowance
                    && Vector3.Distance(levelInfo.blockOne.transform.position, levelInfo.blockThree.transform.position) > values[1] - Allowance &&
                    Vector3.Distance(levelInfo.blockTwo.transform.position, levelInfo.blockFour.transform.position) < values[2] + Allowance
                    && Vector3.Distance(levelInfo.blockTwo.transform.position, levelInfo.blockFour.transform.position) > values[2] -Allowance &&
                    Vector3.Distance(levelInfo.blockTwo.transform.position, levelInfo.blockFive.transform.position) < values[3] + Allowance
                    && Vector3.Distance(levelInfo.blockTwo.transform.position, levelInfo.blockFive.transform.position) > values[3] -Allowance &&
                    Vector3.Distance(levelInfo.blockThree.transform.position, levelInfo.blockFive.transform.position) < values[4] + Allowance
                    && Vector3.Distance(levelInfo.blockThree.transform.position, levelInfo.blockFive.transform.position) > values[4] -Allowance)
                {
                    IsPositionBlockRight = true;
                }
            }
            
            if (levelNumber == 14)
            {
                float[] values = DataGame.CheckDistance(levelNumber);
                if (Vector3.Distance(levelInfo.blockOne.transform.position, levelInfo.blockTwo.transform.position) < values[0] + Allowance
                    && Vector3.Distance(levelInfo.blockOne.transform.position, levelInfo.blockTwo.transform.position) > values[0] - Allowance &&
                    Vector3.Distance(levelInfo.blockOne.transform.position, levelInfo.blockFour.transform.position) < values[1] + Allowance
                    && Vector3.Distance(levelInfo.blockOne.transform.position, levelInfo.blockFour.transform.position) > values[1] - Allowance &&
                    Vector3.Distance(levelInfo.blockTwo.transform.position, levelInfo.blockThree.transform.position) < values[2] + Allowance
                    && Vector3.Distance(levelInfo.blockTwo.transform.position, levelInfo.blockThree.transform.position) > values[2] -Allowance &&
                    Vector3.Distance(levelInfo.blockThree.transform.position, levelInfo.blockFive.transform.position) < values[3] + Allowance
                    && Vector3.Distance(levelInfo.blockThree.transform.position, levelInfo.blockFive.transform.position) > values[3] -Allowance &&
                    Vector3.Distance(levelInfo.blockThree.transform.position, levelInfo.blockFive.transform.position) < values[4] + Allowance
                    && Vector3.Distance(levelInfo.blockThree.transform.position, levelInfo.blockFive.transform.position) > values[4] -Allowance)
                {
                    IsPositionBlockRight = true;
                }
            }
            
            if (levelNumber == 15)
            {
                float[] values = DataGame.CheckDistance(levelNumber);
                if (Vector3.Distance(levelInfo.blockOne.transform.position, levelInfo.blockTwo.transform.position) < values[0] + Allowance
                    && Vector3.Distance(levelInfo.blockOne.transform.position, levelInfo.blockTwo.transform.position) > values[0] - Allowance &&
                    Vector3.Distance(levelInfo.blockTwo.transform.position, levelInfo.blockThree.transform.position) < values[1] + Allowance
                    && Vector3.Distance(levelInfo.blockTwo.transform.position, levelInfo.blockThree.transform.position) > values[1] - Allowance &&
                    Vector3.Distance(levelInfo.blockTwo.transform.position, levelInfo.blockFour.transform.position) < values[2] + Allowance
                    && Vector3.Distance(levelInfo.blockTwo.transform.position, levelInfo.blockFour.transform.position) > values[2] -Allowance &&
                    Vector3.Distance(levelInfo.blockFour.transform.position, levelInfo.blockFive.transform.position) < values[3] + Allowance
                    && Vector3.Distance(levelInfo.blockFour.transform.position, levelInfo.blockFive.transform.position) > values[3] -Allowance)
                {
                    IsPositionBlockRight = true;
                }
                if (Vector3.Distance(levelInfo.blockOne.transform.position, levelInfo.blockTwo.transform.position) < values[0] + Allowance
                    && Vector3.Distance(levelInfo.blockOne.transform.position, levelInfo.blockTwo.transform.position) > values[0] - Allowance &&
                    Vector3.Distance(levelInfo.blockTwo.transform.position, levelInfo.blockThree.transform.position) < values[1] + Allowance
                    && Vector3.Distance(levelInfo.blockTwo.transform.position, levelInfo.blockThree.transform.position) > values[1] - Allowance &&
                    Vector3.Distance(levelInfo.blockTwo.transform.position, levelInfo.blockFive.transform.position) < values[2] + Allowance
                    && Vector3.Distance(levelInfo.blockTwo.transform.position, levelInfo.blockFive.transform.position) > values[2] -Allowance &&
                    Vector3.Distance(levelInfo.blockFour.transform.position, levelInfo.blockFive.transform.position) < values[3] + Allowance
                    && Vector3.Distance(levelInfo.blockFour.transform.position, levelInfo.blockFive.transform.position) > values[3] -Allowance)
                {
                    IsPositionBlockRight = true;
                }
            }
            
            if (levelNumber == 16)
            {
                float[] values = DataGame.CheckDistance(levelNumber);
                if (Vector3.Distance(levelInfo.blockOne.transform.position, levelInfo.blockFour.transform.position) < values[0] + Allowance
                    && Vector3.Distance(levelInfo.blockOne.transform.position, levelInfo.blockFour.transform.position) > values[0] - Allowance &&
                    Vector3.Distance(levelInfo.blockTwo.transform.position, levelInfo.blockFour.transform.position) < values[1] + Allowance
                    && Vector3.Distance(levelInfo.blockTwo.transform.position, levelInfo.blockFour.transform.position) > values[1] - Allowance &&
                    Vector3.Distance(levelInfo.blockFour.transform.position, levelInfo.blockThree.transform.position) < values[2] + Allowance
                    && Vector3.Distance(levelInfo.blockFour.transform.position, levelInfo.blockThree.transform.position) > values[2] -Allowance)
                {
                    IsPositionBlockRight = true;
                }
            }

            if (levelNumber == 17)
            {
                float[] values = DataGame.CheckDistance(levelNumber);
                if (Vector3.Distance(levelInfo.blockOne.transform.position, levelInfo.blockThree.transform.position) < values[0] + Allowance
                    && Vector3.Distance(levelInfo.blockOne.transform.position, levelInfo.blockThree.transform.position) > values[0] - Allowance &&
                    Vector3.Distance(levelInfo.blockThree.transform.position, levelInfo.blockFive.transform.position) < values[1] + Allowance
                    && Vector3.Distance(levelInfo.blockThree.transform.position, levelInfo.blockFive.transform.position) > values[1] - Allowance &&
                    Vector3.Distance(levelInfo.blockTwo.transform.position, levelInfo.blockFive.transform.position) < values[2] + Allowance
                    && Vector3.Distance(levelInfo.blockTwo.transform.position, levelInfo.blockFive.transform.position) > values[2] - Allowance &&
                    Vector3.Distance(levelInfo.blockFour.transform.position, levelInfo.blockFive.transform.position) < values[3] + Allowance
                    && Vector3.Distance(levelInfo.blockFour.transform.position, levelInfo.blockFive.transform.position) > values[3] - Allowance)
                {
                    IsPositionBlockRight = true;
                }

                if (Vector3.Distance(levelInfo.blockTwo.transform.position, levelInfo.blockThree.transform.position) < values[0] + Allowance
                    && Vector3.Distance(levelInfo.blockTwo.transform.position, levelInfo.blockThree.transform.position) > values[0] - Allowance &&
                    Vector3.Distance(levelInfo.blockThree.transform.position, levelInfo.blockFive.transform.position) < values[1] + Allowance
                    && Vector3.Distance(levelInfo.blockThree.transform.position, levelInfo.blockFive.transform.position) > values[1] - Allowance &&
                    Vector3.Distance(levelInfo.blockOne.transform.position, levelInfo.blockFive.transform.position) <  values[2] + Allowance
                    && Vector3.Distance(levelInfo.blockOne.transform.position, levelInfo.blockFive.transform.position) > values[2] - Allowance &&
                    Vector3.Distance(levelInfo.blockFour.transform.position, levelInfo.blockFive.transform.position) < values[3] + Allowance
                    && Vector3.Distance(levelInfo.blockFour.transform.position, levelInfo.blockFive.transform.position) > values[3] - Allowance)
                {
                    IsPositionBlockRight = true;
                }

                if (Vector3.Distance(levelInfo.blockOne.transform.position, levelInfo.blockFive.transform.position) < values[0] + Allowance
                    && Vector3.Distance(levelInfo.blockFive.transform.position, levelInfo.blockFive.transform.position) > values[0] - Allowance &&
                    Vector3.Distance(levelInfo.blockThree.transform.position, levelInfo.blockFive.transform.position) < values[1] + Allowance
                    && Vector3.Distance(levelInfo.blockThree.transform.position,  levelInfo.blockFive.transform.position) > values[1] - Allowance &&
                    Vector3.Distance(levelInfo.blockTwo.transform.position, levelInfo.blockThree.transform.position) < values[2] + Allowance
                    && Vector3.Distance(levelInfo.blockTwo.transform.position, levelInfo.blockThree.transform.position) > values[2] - Allowance &&
                    Vector3.Distance(levelInfo.blockThree.transform.position, levelInfo.blockFour.transform.position) < values[3] + Allowance
                    && Vector3.Distance(levelInfo.blockThree.transform.position, levelInfo.blockFour.transform.position) > values[3] - Allowance)
                {
                    IsPositionBlockRight = true;
                }

                if (Vector3.Distance(levelInfo.blockTwo.transform.position, levelInfo.blockFive.transform.position) < values[0] + Allowance
                    && Vector3.Distance(levelInfo.blockTwo.transform.position, levelInfo.blockFive.transform.position) > values[0] - Allowance &&
                    Vector3.Distance(levelInfo.blockThree.transform.position, levelInfo.blockFive.transform.position) < values[1] + Allowance
                    && Vector3.Distance(levelInfo.blockThree.transform.position, levelInfo.blockFive.transform.position) > values[1] - Allowance &&
                    Vector3.Distance(levelInfo.blockOne.transform.position, levelInfo.blockThree.transform.position) < values[2] + Allowance
                    && Vector3.Distance(levelInfo.blockOne.transform.position, levelInfo.blockThree.transform.position) > values[2] - Allowance &&
                    Vector3.Distance(levelInfo.blockThree.transform.position, levelInfo.blockFour.transform.position) < values[3] + Allowance
                    && Vector3.Distance(levelInfo.blockThree.transform.position, levelInfo.blockFour.transform.position) > values[3] - Allowance)
                {
                    IsPositionBlockRight = true;
                }
            }
            
            if (levelNumber == 18)
            {
                float[] values = DataGame.CheckDistance(levelNumber);
                if (Vector3.Distance(levelInfo.blockOne.transform.position, levelInfo.blockTwo.transform.position) < values[0] + Allowance
                    && Vector3.Distance(levelInfo.blockOne.transform.position, levelInfo.blockTwo.transform.position) > values[0] - Allowance &&
                    Vector3.Distance(levelInfo.blockOne.transform.position, levelInfo.blockThree.transform.position) < values[1] + Allowance
                    && Vector3.Distance(levelInfo.blockOne.transform.position, levelInfo.blockThree.transform.position) > values[1] - Allowance &&
                    Vector3.Distance(levelInfo.blockThree.transform.position, levelInfo.blockFour.transform.position) < values[2] + Allowance
                    && Vector3.Distance(levelInfo.blockThree.transform.position, levelInfo.blockFour.transform.position) > values[2] -Allowance  &&
                    Vector3.Distance(levelInfo.blockFour.transform.position, levelInfo.blockFive.transform.position) < values[3] + Allowance
                    && Vector3.Distance(levelInfo.blockFour.transform.position, levelInfo.blockFive.transform.position) > values[3] -Allowance)
                {
                    IsPositionBlockRight = true;
                }
            }

            if (levelNumber == 19)
            {
                float[] values = DataGame.CheckDistance(levelNumber);
                if (Vector3.Distance(levelInfo.blockOne.transform.position, levelInfo.blockTwo.transform.position) < values[0] + Allowance
                    && Vector3.Distance(levelInfo.blockOne.transform.position, levelInfo.blockTwo.transform.position) > values[0] - Allowance &&
                    Vector3.Distance(levelInfo.blockOne.transform.position, levelInfo.blockFour.transform.position) < values[1] + Allowance
                    && Vector3.Distance(levelInfo.blockOne.transform.position, levelInfo.blockFour.transform.position) > values[1] - Allowance &&
                    Vector3.Distance(levelInfo.blockFour.transform.position, levelInfo.blockFive.transform.position) < values[2] + AllowanceLong
                    && Vector3.Distance(levelInfo.blockFour.transform.position, levelInfo.blockFive.transform.position) > values[2] -AllowanceLong  &&
                    Vector3.Distance(levelInfo.blockFour.transform.position, levelInfo.blockThree.transform.position) < values[3] + Allowance
                    && Vector3.Distance(levelInfo.blockFour.transform.position, levelInfo.blockThree.transform.position) > values[3] -Allowance)
                {
                    IsPositionBlockRight = true;
                }
            }
            
            if (levelNumber == 20)
            {
                float[] values = DataGame.CheckDistance(levelNumber);
                if (Vector3.Distance(levelInfo.blockOne.transform.position, levelInfo.blockTwo.transform.position) < values[0] + Allowance
                    && Vector3.Distance(levelInfo.blockOne.transform.position, levelInfo.blockTwo.transform.position) > values[0] - Allowance &&
                    Vector3.Distance(levelInfo.blockTwo.transform.position, levelInfo.blockThree.transform.position) < values[1] + Allowance
                    && Vector3.Distance(levelInfo.blockTwo.transform.position, levelInfo.blockThree.transform.position) > values[1] - Allowance &&
                    Vector3.Distance(levelInfo.blockThree.transform.position, levelInfo.blockFour.transform.position) < values[2] + AllowanceLong
                    && Vector3.Distance(levelInfo.blockThree.transform.position, levelInfo.blockFour.transform.position) > values[2] -AllowanceLong  &&
                    Vector3.Distance(levelInfo.blockThree.transform.position, levelInfo.blockFive.transform.position) < values[3] + Allowance
                    && Vector3.Distance(levelInfo.blockThree.transform.position, levelInfo.blockFive.transform.position) > values[3] -Allowance)
                {
                    IsPositionBlockRight = true;
                }
                
                if (Vector3.Distance(levelInfo.blockOne.transform.position, levelInfo.blockTwo.transform.position) < values[0] + Allowance
                    && Vector3.Distance(levelInfo.blockOne.transform.position, levelInfo.blockTwo.transform.position) > values[0] - Allowance &&
                    Vector3.Distance(levelInfo.blockOne.transform.position, levelInfo.blockThree.transform.position) < values[1] + Allowance
                    && Vector3.Distance(levelInfo.blockOne.transform.position, levelInfo.blockThree.transform.position) > values[1] - Allowance &&
                    Vector3.Distance(levelInfo.blockThree.transform.position, levelInfo.blockFour.transform.position) < values[2] + AllowanceLong
                    && Vector3.Distance(levelInfo.blockThree.transform.position, levelInfo.blockFour.transform.position) > values[2] -AllowanceLong  &&
                    Vector3.Distance(levelInfo.blockThree.transform.position, levelInfo.blockFive.transform.position) < values[3] + Allowance
                    && Vector3.Distance(levelInfo.blockThree.transform.position, levelInfo.blockFive.transform.position) > values[3] -Allowance)
                {
                    IsPositionBlockRight = true;
                }
            }
            
            if (levelNumber == 21)
            {
                float[] values = DataGame.CheckDistance(levelNumber);
                if (Vector3.Distance(levelInfo.blockOne.transform.position, levelInfo.blockThree.transform.position) < values[0] + Allowance
                    && Vector3.Distance(levelInfo.blockOne.transform.position, levelInfo.blockThree.transform.position) > values[0] - Allowance &&
                    Vector3.Distance(levelInfo.blockOne.transform.position, levelInfo.blockFour.transform.position) < values[1] + Allowance
                    && Vector3.Distance(levelInfo.blockOne.transform.position, levelInfo.blockFour.transform.position) > values[1] - Allowance &&
                    Vector3.Distance(levelInfo.blockOne.transform.position, levelInfo.blockFive.transform.position) < values[2] + AllowanceLong
                    && Vector3.Distance(levelInfo.blockOne.transform.position, levelInfo.blockFive.transform.position) > values[2] -AllowanceLong  &&
                    Vector3.Distance(levelInfo.blockFour.transform.position, levelInfo.blockTwo.transform.position) < values[3] + Allowance
                    && Vector3.Distance(levelInfo.blockFour.transform.position, levelInfo.blockTwo.transform.position) > values[3] -Allowance)
                {
                    IsPositionBlockRight = true;
                }
                
                if (Vector3.Distance(levelInfo.blockOne.transform.position, levelInfo.blockTwo.transform.position) < values[0] + Allowance
                    && Vector3.Distance(levelInfo.blockOne.transform.position, levelInfo.blockTwo.transform.position) > values[0] - Allowance &&
                    Vector3.Distance(levelInfo.blockOne.transform.position, levelInfo.blockFour.transform.position) < values[1] + Allowance
                    && Vector3.Distance(levelInfo.blockOne.transform.position, levelInfo.blockFour.transform.position) > values[1] - Allowance &&
                    Vector3.Distance(levelInfo.blockOne.transform.position, levelInfo.blockFive.transform.position) < values[2] + AllowanceLong
                    && Vector3.Distance(levelInfo.blockOne.transform.position, levelInfo.blockFive.transform.position) > values[2] -AllowanceLong  &&
                    Vector3.Distance(levelInfo.blockFour.transform.position, levelInfo.blockThree.transform.position) < values[3] + Allowance
                    && Vector3.Distance(levelInfo.blockFour.transform.position, levelInfo.blockThree.transform.position) > values[3] -Allowance)
                {
                    IsPositionBlockRight = true;
                }
            }
            Debug.Log(Vector3.Distance(levelInfo.blockOne.transform.position, levelInfo.blockTwo.transform.position));
            Debug.Log(Vector3.Distance(levelInfo.blockOne.transform.position, levelInfo.blockThree.transform.position));
            Debug.Log(Vector3.Distance(levelInfo.blockThree.transform.position, levelInfo.blockFour.transform.position));
            Debug.Log(Vector3.Distance(levelInfo.blockFour.transform.position, levelInfo.blockFive.transform.position));
            return IsPositionBlockRight;
        }
    }
}