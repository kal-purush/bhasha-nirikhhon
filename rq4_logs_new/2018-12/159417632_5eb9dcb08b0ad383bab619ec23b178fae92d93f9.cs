using System;
using System.Collections.Generic;
using Microsoft.Xna.Framework;
using Microsoft.Xna.Framework.Graphics;
using Microsoft.Xna.Framework.Input;

namespace GSeoFinalProject
{
    class Enemy: DrawableGameComponent
    {
        Game1 game;
        internal Rectangle rectangle;
        Vector2 position;
        Texture2D explodedEnemy;
        bool isHit = false;
        int timerSinceHit;

        public Enemy(Game1 game) : base(game)
        {
            this.game = game;
        }
    }
}