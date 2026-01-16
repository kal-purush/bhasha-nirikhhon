using System;
using System.Collections.Generic;
using SFML.Graphics;
using SFML.System;

namespace Runner
{
    class Obstacle
    {

        //TODO: add collisions
        public Sprite sprite;
        public int score = 1;
        public float scrollSpeedToAdd = 1f;

        //private Level level;

        public Obstacle(Level level, Sprite sprite, Vector2f position)
        {
            this.sprite = sprite;
            //this.level = level;

            this.sprite.Position = position;

            level.obstacles.Add(this);
        }
    }
}