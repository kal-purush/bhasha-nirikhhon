using System.Drawing;

namespace PingPongGame.models
{
    public class BallComponent : GameComponent
    {
        public void Update()
        {
            this.Position = new Point(x: Position.X + Velocity.X, y: Position.Y + Velocity.Y);
        }
    }
}