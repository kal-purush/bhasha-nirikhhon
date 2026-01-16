using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

using Microsoft.Xna.Framework;
using Microsoft.Xna.Framework.Graphics;

using RkoOuttaNowhere.Screens;
using RkoOuttaNowhere.Images;

namespace RkoOuttaNowhere.Ui
{
    class GameplayGui : Gui
    {
        private GuiPanel _topBar;
        private Vector2 _position, _originOffset, _imageOffset;

        public GameplayGui()
            : base()
        {
            _panels = new List<GuiPanel>();
            _topBar = new GuiPanel();
            _position = _imageOffset = _originOffset = Vector2.Zero;
        }

        public override void LoadContent()
        {
            base.LoadContent();
            // Add our panels
            _panels.Add(_topBar);

            // Inintialize our offsets
            _position = Vector2.Zero;
            _imageOffset = new Vector2(75, 0);
            _originOffset = new Vector2(10, 10);

            // Timer
            Image timer = new Image();
            timer.Path = "ui/gameplay/timer";
            timer.Position = _position + _originOffset;
            timer.LoadContent();
            _topBar.Images.Add(timer);

            // Countdown
            Image countdown = new Image();
            countdown.Path = "transparent";
            countdown.Position = _position + _originOffset + _imageOffset + new Vector2(7, 17);
            countdown.Text = "99";
            countdown.Scale = new Vector2(2, 2);
            countdown.LoadContent();
            _topBar.Images.Add(countdown);
            
        }

        public override void UnloadContent()
        {
            base.UnloadContent();
        }

        public void Update(GameTime gameTime)
        {
            base.Update(gameTime);
        }

        public override void Draw(SpriteBatch spriteBatch)
        {
            base.Draw(spriteBatch);
        }

        public override void SetTimer(float countdown)
        {
            _topBar.Images[1].Text = ((int)(countdown)).ToString();
            _topBar.Images[1].LoadContent();
        }

        public void HandleNewGame(object sender, EventArgs e)
        {
            Console.WriteLine("New Game");
        }
    }
}