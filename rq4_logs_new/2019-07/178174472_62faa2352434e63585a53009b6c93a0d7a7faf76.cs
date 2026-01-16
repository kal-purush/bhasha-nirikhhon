using Microsoft.Xna.Framework;
using Microsoft.Xna.Framework.Input;
using MononokeEngine;
using MononokeEngine.ECS;
using MononokeEngine.Input;
using MononokeEngine.Scenes;

namespace Sandbox.Components
{
    public class CameraController : Component
    {
        public override void Update()
        {
            base.Update();
            
            KeyboardInput input = MononokeEngine.Mononoke.Input.Keyboard;
            Camera cam = Mononoke.Scenes.Current.MainCamera;
            Vector2 pos = cam.Position;
            
            if (input.IsKeyHeld(Keys.Right))
                pos.X++;
            
            if (input.IsKeyHeld(Keys.Left))
                pos.X--;
            
            if (input.IsKeyHeld(Keys.Down))
                pos.Y++;
            
            if (input.IsKeyHeld(Keys.Up))
                pos.Y--;

            cam.Position = pos;
        }
    }
}