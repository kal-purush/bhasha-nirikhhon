using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.Xna.Framework;
using Microsoft.Xna.Framework.Graphics;
using Microsoft.Xna.Framework.Content;

namespace myGame
{
    class PowerUp : GameObject
    {
        private static System.Random rnd = new System.Random((int)DateTime.Now.Ticks);
        private Texture2D powerUpLogo;
        private string textureName;

        public int x;
        public int y;
        public int width;
        public int height;

        public PowerUp(KittenWarsGame game)
            : base(game)
        {
            this.textureName = "placeholder";
        }

        public Rectangle Rectangle
        {
            get { return new Rectangle(x, y, width, height); }
        }

        public override void Initialize()
        {
            width = 50;
            height = 50;
            x = rnd.Next(0, 800);
            y = rnd.Next(50, 500);
        }

        public override void LoadContent()
        {
            powerUpLogo = Content.Load<Texture2D>(textureName);
        }

        public override void Draw(GameTime gameTime)
        {
            SpriteBatch.Begin();
            SpriteBatch.Draw(powerUpLogo, new Rectangle(x, y, width, height), Color.White);
            SpriteBatch.End();
        }

        public override void Update(GameTime gameTime)
        {
            y++;
            if (y == 480)
            {
                y = 50;
            }
        }

        protected bool collisionDetection(Gun gun)
        {
            return gun.Rectangle.Intersects(this.Rectangle);
        }
    }
}