using Microsoft.Xna.Framework;
using Microsoft.Xna.Framework.Graphics;
using Microsoft.Xna.Framework.Input;

namespace GSeoFinalProject
{
    public class EndGameScene : GameScene
    {
        public EndGameScene(Game game) : base(game)
        {
        }

        public override void Initialize()
        {
            this.SceneComponents.Add(new EndGameTextComponent(game));
            this.Hide();
            base.Initialize();
        }

        public override void Update(GameTime gameTime)
        {
            if (Enabled)
            {
                if (Keyboard.GetState().IsKeyDown(Keys.Escape))
                {
                    if (Keyboard.GetState().IsKeyDown(Keys.Escape))
                    {
                        game.HideAllScenes();
                        game.Services.GetService<StartScene>().Show();
                    }
                }
            }
            base.Update(gameTime);
        }


    }
}