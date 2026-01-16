using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.Xna.Framework;
using Microsoft.Xna.Framework.Graphics;
using Microsoft.Xna.Framework.Input;

namespace SpaceInvader
{
    class Player
    {     
        private Game _game;
        private int _pv;
        private Vector2 _pos;
        private Texture2D _texture;
        private int _speed;
        

        int screenSizeWidth = GraphicsAdapter.DefaultAdapter.CurrentDisplayMode.Width;
        int screenSizeHeight = GraphicsAdapter.DefaultAdapter.CurrentDisplayMode.Height;

        public int Speed { get => _speed; set => _speed = value; }

        public Player(int pv,int speed, Texture2D texture, Vector2 position, Game g)
        {
            _texture = texture;
            _pv = 0;            
            _pos = position;
            _game = g;
            _speed = speed;
        }

        public Rectangle BoxCollider
        {
            get
            {
                return new Rectangle((int)_pos.X + 20, (int)_pos.Y + 20, _texture.Width - 20, _texture.Height - 20);
            }
        }

        public int Pv { get => _pv; set => _pv = value; }
        public Vector2 Pos { get => _pos; set => _pos = value; }

        public void Update(GameTime gameTime,KeyboardState kbState)
        {
            
            if (_pos.X > 0)
                if (kbState.IsKeyDown(Keys.A) || kbState.IsKeyDown(Keys.Left))
                    _pos.X -= _speed;

            if (_pos.Y > 0)
                if (kbState.IsKeyDown(Keys.W) || kbState.IsKeyDown(Keys.Up))
                    _pos.Y -= _speed;
            if (_texture != null)
            {
                if (_pos.Y + _texture.Height < screenSizeHeight)
                    if (kbState.IsKeyDown(Keys.S) || kbState.IsKeyDown(Keys.Down))
                        _pos.Y += _speed;

                if (_pos.X + _texture.Width < screenSizeWidth)
                    if (kbState.IsKeyDown(Keys.D) || kbState.IsKeyDown(Keys.Right))
                        _pos.X += _speed;
            }            
        } 
        public void Draw(SpriteBatch spriteBatch)
        {
            if (_texture != null)
            {
                spriteBatch.Draw(_texture, _pos, Color.White);
            }
            
        }
        public Vector2 getPlayerPos()
        {
            return _pos;
        }

        public Texture2D getPlayerTexture()
        {
            return _texture;
        }

        public int getPlayerPv()
        {
            return _pv;
        }
    }
}