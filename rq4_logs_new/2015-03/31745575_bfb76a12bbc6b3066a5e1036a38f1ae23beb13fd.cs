using Microsoft.Xna.Framework;
using Microsoft.Xna.Framework.Graphics;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Keeper.VisualControlers.Player {
    class Turret {
        public Texture2D texture { get; set; }
        public Vector2 position { get; set; }
        public bool active { get; set; }
        private float rotation = 0f;

        public void Initialize(Texture2D texture, Vector2 position) {
            this.position = position;
            this.texture = texture;
            this.active = true;
        }

        public void Draw(SpriteBatch spriteBatch) {
            spriteBatch.Draw(texture, position, null, Color.White, rotation, new Vector2(75), 1f,
               SpriteEffects.None, 0f);
        }

        public void Update(GameTime gameTime, Vector2 position, float rotation, bool firing) {
            this.position = position;
            if (firing) {
                this.rotation = rotation;
            }
        }

        private Vector2 angleToVector(float angle) {
            return new Vector2((float)Math.Cos(angle), (float)Math.Sin(angle));
        }
    }
}