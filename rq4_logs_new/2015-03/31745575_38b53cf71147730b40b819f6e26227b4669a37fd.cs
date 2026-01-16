using Microsoft.Xna.Framework;
using Microsoft.Xna.Framework.Graphics;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Keeper.VisualControlers.Projectiles {
    class Bullet {
        public Texture2D texture { get; set; }
        public Vector2 position;
        public bool active { get; set; }
        private float rotation = 0f;
        private Vector2 movementVector;
        private float speed = 0;

        public void Initialize(Texture2D texture, Vector2 position, float movementAngle, float movementSpeed) {
            this.texture = texture;
            this.rotation = movementAngle;
            movementVector = angleToVector(this.rotation);
            this.position = position + (movementVector * 50);
            speed = movementSpeed;
            this.active = true;
        }

        public void Draw(SpriteBatch spriteBatch) {
            spriteBatch.Draw(texture, position, null, Color.White, rotation, new Vector2(15), 1f,
               SpriteEffects.None, 0f);
        }

        public void Update(GameTime gameTime, Rectangle screen) {
            position.X += (movementVector.X * speed);
            position.Y += (movementVector.Y * speed);
            if (!screen.Contains(position)) {
                active = false;
            }
        }

        private Vector2 angleToVector(float angle) {
            return new Vector2((float)Math.Cos(angle), (float)Math.Sin(angle));
        }
    }
}