using Microsoft.Xna.Framework;
using Microsoft.Xna.Framework.Graphics;
using Microsoft.Xna.Framework.Input;

namespace PixelHunter1995.GameStates
{
    /// <summary>
    /// State when being in main menu
    /// </summary>
    class Menu : IGameState
    {
        private readonly Texture2D menu;
        private readonly SpriteBatch spriteBatch;
        private StateManager stateManager;
        private bool escapeHasBeenUp;

        public Menu(StateManager stateManager, Texture2D menu, SpriteBatch spriteBatch)
        {
            this.stateManager = stateManager;
            this.menu = menu;
            this.spriteBatch = spriteBatch;
            this.escapeHasBeenUp = false;
        }

        public void Draw(GameTime gameTime)
        {
            spriteBatch.Draw(menu, Vector2.Zero, Color.White);
        }

        public void Update(GameTime gameTime)
        {
            KeyboardState state = Keyboard.GetState();
            if (state.IsKeyUp(Keys.Escape))
            {
                escapeHasBeenUp = true;
            }

            if (state.IsKeyDown(Keys.Escape) && escapeHasBeenUp)
            {
                stateManager.SetExit();
            }
            else if (state.IsKeyDown(Keys.Enter))
            {
                escapeHasBeenUp = false;
                stateManager.SetStatePlaying();
            }
        }
    }
}