using Microsoft.Xna.Framework;
using Microsoft.Xna.Framework.Graphics;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace DefendTheBase
{
    class Enemy
    {
        //insert stats: hp, speed, dmg etc.


        Texture2D sprite;
        Coordinates enemyPos;
        Vector2 enemyVect;
        ai pathFinder;

        bool pathFound = false;
        bool destReached = false;

        public Enemy()
        {
            enemyVect = new Vector2(0, 0);
            enemyPos = new Coordinates(0, 0);
            pathFinder = new ai(enemyPos, GameRoot.DEFAULYDIST);
            sprite = Art.EnemyTex;



        }

        public void Update(Grid.gridFlags endPoint)
        {
            if (!pathFound && endPoint.HasFlag(Grid.gridFlags.endPoint))
                pathFound = pathFinder.FindPath(GameRoot.grid.stopPointCoord, GameRoot.grid.gridSquares, GameRoot.HEIGHT, GameRoot.WIDTH);

            if (pathFound)
            {
                pathFinder.PathMove(GameRoot.grid.stopPointCoord, GameRoot.grid.gridSquares, GameRoot.HEIGHT, GameRoot.WIDTH, ref enemyVect);
                enemyPos = pathFinder.aiPos;
            }

            if (GameRoot.grid.stopPointCoord != null)
            {
                if (enemyVect.X == GameRoot.grid.stopPointCoord.x && enemyVect.Y == GameRoot.grid.stopPointCoord.y)
                {
                    enemyPos = new Coordinates(0, 0);
                    enemyVect = new Vector2(0, 0);
                    pathFinder.Reset();
                    pathFinder.aiPos = enemyPos;
                }
            }
        }

        public void Draw(SpriteBatch sb)
        {
            sb.Draw(sprite, new Vector2((int)GameRoot.grid.gridBorder.X + (enemyVect.X * GameRoot.SQUARESIZE), (int)GameRoot.grid.gridBorder.Y + (enemyVect.Y * GameRoot.SQUARESIZE)), Color.Aqua);

        }
    }
}