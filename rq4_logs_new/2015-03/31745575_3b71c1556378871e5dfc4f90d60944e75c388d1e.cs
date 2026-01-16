using Microsoft.Xna.Framework;
using Microsoft.Xna.Framework.Graphics;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Keeper.VisualControlers {
    interface IVisualControl {
        Texture2D texture { get; set; }
        Vector2 position { get; set; }
        bool active { get; set; }

        void Initialize(Microsoft.Xna.Framework.Graphics.Texture2D texture, Microsoft.Xna.Framework.Vector2 position);
        void Draw(Microsoft.Xna.Framework.Graphics.SpriteBatch spriteBatch);
        void Update(Microsoft.Xna.Framework.GameTime gameTime);
    }
}