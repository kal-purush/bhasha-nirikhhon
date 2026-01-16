using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Microsoft.Xna.Framework;
using Microsoft.Xna.Framework.Audio;
using Microsoft.Xna.Framework.Content;
using Microsoft.Xna.Framework.GamerServices;
using Microsoft.Xna.Framework.Graphics;
using Microsoft.Xna.Framework.Input;
using Microsoft.Xna.Framework.Media;
using Assignment3.Scenes;

namespace Assignment3.Entities
{
    public class Wall : Entity
    {
        private Model model;

        public Wall(ContentManager content)
        {
            model = content.Load<Model>("Models/Wall");
        }
        
        public override void update(GameTime gameTime, GamePadState gamepad, KeyboardState keyboard)
        {

        }

        public override void draw(SpriteBatch sb)
        {
            // Copy any parent transforms.
            Matrix[] transforms = new Matrix[model.Bones.Count];
            model.CopyAbsoluteBoneTransformsTo(transforms);

            // Draw the model. A model can have multiple meshes, so loop.
            foreach (ModelMesh mesh in model.Meshes)
            {

                // This is where the mesh orientation is set, as well as our camera and projection.
                foreach (BasicEffect effect in mesh.Effects)
                {
                    effect.EnableDefaultLighting();//lighting

                    effect.World = transforms[mesh.ParentBone.Index] * Matrix.CreateScale(0.01f, 0.01f, 0.01f)
                        * Matrix.CreateTranslation(Vector3.Zero);
                    effect.View = MazeScene.instance.camera.View;
                    effect.Projection = MazeScene.instance.camera.Projection;
                }
                // Draw the mesh, using the effects set above.
                mesh.Draw();
            }
        }
    }
}