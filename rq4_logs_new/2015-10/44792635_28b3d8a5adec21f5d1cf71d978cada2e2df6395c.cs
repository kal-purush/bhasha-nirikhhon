using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Microsoft.Xna.Framework;
using Microsoft.Xna.Framework.Graphics;
using Microsoft.Xna.Framework.Input;
using Microsoft.Xna.Framework.Content;
using Assignment3.Scenes;

namespace Assignment3.Entities
{
    class Player : Entity
    {
        public Model playerModel;
        public Vector3 position;

        public Player()
        {
            position = MazeScene.instance.camera.Position;
        }

        public void Load(ContentManager cm)
        {
            playerModel = cm.Load<Model>("Models/Player");
        }

        public override void draw(SpriteBatch sb)
        {
            //nothing needed
        }

        public override void update(GameTime gameTime, GamePadState gamepad, KeyboardState keyboard)
        {
            //update model position w/ camera
            position = MazeScene.instance.camera.Position;
        }
    }
}