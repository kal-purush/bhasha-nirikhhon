using Microsoft.Xna.Framework;
using Microsoft.Xna.Framework.Graphics;
using Microsoft.Xna.Framework.Input.Touch;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Keeper.VisualControlers.Controls {
    class Joystick {
        public Texture2D texture { get; set; }
        public Vector2 position { get; set; }
        public bool active { get; set; }
        public Rectangle area { get; set; }
        public float rotationOutput { get; set; }
        public float length { get; set; }

        public void Initialize(Texture2D texture, Vector2 position) {
            this.position = position;
            this.texture = texture;
            this.active = true;
            this.area = new Rectangle((int)position.X, (int)position.Y, 500, 500);
            rotationOutput = 0f;
            length = 0f;
        }

        public void Draw(SpriteBatch spriteBatch) {
            spriteBatch.Draw(texture, position, null, Color.White, 0f, Vector2.Zero, 1f,
               SpriteEffects.None, 0f);
        }

        public void Update(GameTime gameTime) {
            TouchCollection touches = TouchPanel.GetState();
            if (touches.Any(x => area.Contains(x.Position))) {
                TouchLocation touch = touches.First(x => area.Contains(x.Position));
                Vector2 center = new Vector2(area.Center.X, area.Center.Y);
                Vector2 movementVector = new Vector2(touch.Position.X - center.X, touch.Position.Y - center.Y);
                rotationOutput = vectorToAngle(movementVector);
                length = movementVector.Length();
            } else {
                rotationOutput = 0f;
                length = -1f;
            }
        }

        private float vectorToAngle(Vector2 vector) {
            return (float)Math.Atan2(vector.Y, vector.X);
        }
    }
}