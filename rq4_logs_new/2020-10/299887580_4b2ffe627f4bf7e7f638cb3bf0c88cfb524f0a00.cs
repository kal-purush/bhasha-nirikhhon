using GameDev_Olivier_DuFour_2EACL1.Interfaces;
using Microsoft.Xna.Framework;
using Microsoft.Xna.Framework.Graphics;
using System;
using System.Collections.Generic;
using System.Text;

namespace GameDev_Olivier_DuFour_2EACL1
{
    public class Player:IGameObject
    {
        Texture2D playerTexture;
        private Rectangle deelRectangle;
        private int schuifOp_X = 0;
        public Player(Texture2D text)
        {
            playerTexture = text;
            deelRectangle = new Rectangle(schuifOp_X, 0, 108, 140);
        }

        public void Update()
        {
            schuifOp_X += 108;
            if (schuifOp_X > 756)
            {
                schuifOp_X = 0;
            }
            deelRectangle.X = schuifOp_X;
        }

        public void Draw(SpriteBatch _spriteBatch)
        {
            _spriteBatch.Draw(playerTexture, new Vector2(10, 10), deelRectangle, Color.White);
        }
    }
}