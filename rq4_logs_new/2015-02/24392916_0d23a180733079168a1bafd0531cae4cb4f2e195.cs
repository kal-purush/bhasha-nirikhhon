using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Pong.Sprites;
using Microsoft.Xna.Framework;
using Microsoft.Xna.Framework.Graphics;
using Microsoft.Xna.Framework.Input;
using Pong.Screens;
using FontEffectsLib.SpriteTypes;

namespace Pong.CoreTypes
{
    class OnlineJoinScreen : BaseScreen
    {

        Button mainMenuButton;

        GamePadMapper gamePad;

        public override void Load(Microsoft.Xna.Framework.Content.ContentManager Content)
        {
            gamePad = new GamePadMapper(PlayerIndex.One);

            mainMenuButton = new Button(Content.Load<Texture2D>("Buttons//Menu"), new Vector2(0, 0), Color.White, new Rectangle(0, 149, 707, 169), new Rectangle(0, 0, 707, 149));
            mainMenuButton.Origin = new Vector2(mainMenuButton.Texture.Width / 2, 169);
            mainMenuButton.Position = new Vector2(960, 469 + mainMenuButton.SourceRectangle.Value.Height / 2);

            _sprites.Add(mainMenuButton);

            if (!Global.UsingKeyboard)
            {
                mainMenuButton.IsPressed = true;
            }
        }

        public override void Update(GameTime gameTime)
        {

            if (Global.UsingKeyboard)
            {
                if (InputManager.JustPressed(Keys.Escape))
                {
                    ScreenManager.Change(ScreenState.MainMenu);
                }
                if (mainMenuButton.IsClicked)
                {
                    ScreenManager.Change(ScreenState.MainMenu);
                }
            }
            else
            {
                if (InputManager.IsGamepadButtonTapped(PlayerIndex.One, GamePadMapper.GamePadButtons.Back))
                {
                    ScreenManager.Back();
                }

                if (InputManager.IsGamepadButtonTapped(PlayerIndex.One, GamePadMapper.GamePadButtons.A))
                {
                    ScreenManager.Change(ScreenState.MainMenu);
                }

            }
            base.Update(gameTime);
        }
    }
}