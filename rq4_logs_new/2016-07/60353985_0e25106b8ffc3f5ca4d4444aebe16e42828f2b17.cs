using GameOne.Source.Enumerations;
using Microsoft.Xna.Framework.Content;
using Microsoft.Xna.Framework.Input;

namespace GameOne.Source.UI
{
    using Microsoft.Xna.Framework;
    using Microsoft.Xna.Framework.Graphics;

    public class MainMenu
    {
        #region Fields

        // textures

        private ContentManager content;

        #endregion Fields

        //===================================================================

        #region Constructors

        public MainMenu(ContentManager content)
        {
            this.content = content;
            this.MainManuScreen = content.Load<Texture2D>("Images/Menu/MainMenu");
        }

        #endregion Constructors

        //===================================================================

        #region Properties

        public Texture2D MainManuScreen { get; set; }

        #endregion Properties

        //===================================================================

        #region Methods

        public GameState Update(GameTime gameTime)
        {
            // Respond to user input for menu selections, etc

            //Start New Game or Resume curren
            if ((this.MainManuScreen == this.content.Load<Texture2D>("Images/Menu/MainMenu") || this.MainManuScreen == this.content.Load<Texture2D>("Images/Menu/ResumeGame")) &&
                Mouse.GetState().LeftButton == ButtonState.Pressed &&
                Mouse.GetState().X >= 250 &&
                Mouse.GetState().X <= 550 &&
                Mouse.GetState().Y >= 100 &&
                Mouse.GetState().Y <= 140)
            {
                return GameState.Gameplay;
            }
            //Exit Game
            else if ((this.MainManuScreen == this.content.Load<Texture2D>("Images/Menu/MainMenu") || this.MainManuScreen == this.content.Load<Texture2D>("Images/Menu/ResumeGame")) &&
                Mouse.GetState().LeftButton == ButtonState.Pressed &&
                Mouse.GetState().X >= 250 &&
                Mouse.GetState().X <= 550 &&
                Mouse.GetState().Y >= 350 &&
                Mouse.GetState().Y <= 390)
            {
                System.Environment.Exit(1);
                //We may add "Are you sure", "Do you want to save?" or something...
            }
            //Credits
            else if ((this.MainManuScreen == this.content.Load<Texture2D>("Images/Menu/MainMenu") || this.MainManuScreen == this.content.Load<Texture2D>("Images/Menu/ResumeGame")) &&
                Mouse.GetState().LeftButton == ButtonState.Pressed &&
                Mouse.GetState().X >= 250 &&
                Mouse.GetState().X <= 550 &&
                Mouse.GetState().Y >= 300 &&
                Mouse.GetState().Y <= 340)
            {
                this.MainManuScreen = this.content.Load<Texture2D>("Images/Menu/Credits");
                return GameState.MainMenu;
            }
            //Back From Credits or Save or Load. If we start New Game, then go to main menue, then press Credits, then back, first button will be New Game, but it will actualy Resume current game. I will fix it later<=====================================================================
            else if ((this.MainManuScreen == this.content.Load<Texture2D>("Images/Menu/Credits") || this.MainManuScreen == this.content.Load<Texture2D>("Images/Menu/WePromise")) &&
                Mouse.GetState().LeftButton == ButtonState.Pressed &&
                Mouse.GetState().X >= 250 &&
                Mouse.GetState().X <= 550 &&
                Mouse.GetState().Y >= 400 &&
                Mouse.GetState().Y <= 450)
            {
                this.MainManuScreen = this.content.Load<Texture2D>("Images/Menu/MainMenu");
                return GameState.MainMenu;
            }
            else if ((this.MainManuScreen == this.content.Load<Texture2D>("Images/Menu/Credits") || this.MainManuScreen == this.content.Load<Texture2D>("Images/Menu/WePromise")) &&
                Keyboard.GetState().IsKeyDown(Keys.Escape))
            {
                this.MainManuScreen = this.content.Load<Texture2D>("Images/Menu/MainMenu");
                return GameState.MainMenu;
            }
            //Save TODO
            else if ((this.MainManuScreen == this.content.Load<Texture2D>("Images/Menu/MainMenu") || this.MainManuScreen == this.content.Load<Texture2D>("Images/Menu/ResumeGame")) &&
                Mouse.GetState().LeftButton == ButtonState.Pressed &&
                Mouse.GetState().X >= 250 &&
                Mouse.GetState().X <= 550 &&
                Mouse.GetState().Y >= 150 &&
                Mouse.GetState().Y <= 190)
            {
                this.MainManuScreen = this.content.Load<Texture2D>("Images/Menu/WePromise");
                return GameState.MainMenu;
            }
            //Load TODO
            else if ((this.MainManuScreen == this.content.Load<Texture2D>("Images/Menu/MainMenu") || this.MainManuScreen == this.content.Load<Texture2D>("Images/Menu/ResumeGame")) &&
                Mouse.GetState().LeftButton == ButtonState.Pressed &&
                Mouse.GetState().X >= 250 &&
                Mouse.GetState().X <= 550 &&
                Mouse.GetState().Y >= 200 &&
                Mouse.GetState().Y <= 240)
            {
                this.MainManuScreen = this.content.Load<Texture2D>("Images/Menu/WePromise");
                return GameState.MainMenu;
            }

            return GameState.MainMenu;
        }


        #endregion Methods

        public void Draw(GraphicsDevice graphicsDevice, SpriteBatch spriteBatch)
        {
            graphicsDevice.Clear(Color.CornflowerBlue);
            spriteBatch.Begin();
            spriteBatch.Draw(this.MainManuScreen, Vector2.Zero);
            spriteBatch.End();
        }
    }
}