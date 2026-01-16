namespace LoZClone
{
    using Microsoft.Xna.Framework;
    using Microsoft.Xna.Framework.Graphics;

    public class HealthRestoreGameState : GameStateEssentials, IGameState
    {
        private int lifeTime;
        private readonly int healthRestoreTime = ((LoZGame.Instance.Players[0].Health.MaxHealth - LoZGame.Instance.Players[0].Health.CurrentHealth) / 4) * (LoZGame.Instance.UpdateSpeed / 2); // should be dependent on missing player health

        public HealthRestoreGameState()
        {
            lifeTime = 0;

            // Account for the fact that health doesn't have to be lost in whole-heart intervals.
            if ((LoZGame.Instance.Players[0].Health.MaxHealth - LoZGame.Instance.Players[0].Health.CurrentHealth) % 4 != 0)
            {
                healthRestoreTime += LoZGame.Instance.UpdateSpeed / 2;
            }

            foreach (IPlayer player in LoZGame.Instance.Players)
            {
                player.Physics.KnockbackVelocity = Vector2.Zero;
            }
        }

        /// <inheritdoc></inheritdoc>
        public override void PlayGame()
        {
            LoZGame.Instance.GameState = new PlayGameState();
        }

        /// <inheritdoc></inheritdoc>
        public override void Update()
        {
            lifeTime++;
            if (lifeTime > healthRestoreTime)
            {
                PlayGame();
            }
            else
            {
                if (lifeTime % (LoZGame.Instance.UpdateSpeed / 2) == 0)
                {
                    // Restore one full heart per tick unless there is less than one heart still missing, in which case just top the player off.
                    if (LoZGame.Instance.Players[0].Health.MaxHealth - LoZGame.Instance.Players[0].Health.CurrentHealth < 4)
                    {
                        LoZGame.Instance.Players[0].Health.CurrentHealth = LoZGame.Instance.Players[0].Health.MaxHealth;
                    }
                    else
                    {
                        LoZGame.Instance.Players[0].Health.CurrentHealth += 4;
                    }
                    SoundFactory.Instance.PlayGetHeartOrKey();
                }
            }
        }

        /// <inheritdoc></inheritdoc>
        public override void Draw()
        {
            LoZGame.Instance.SpriteBatch.Begin(SpriteSortMode.FrontToBack, BlendState.NonPremultiplied, SamplerState.PointClamp, DepthStencilState.DepthRead, RasterizerState.CullNone, LoZGame.Instance.BetterTinting);
            LoZGame.Instance.Dungeon.CurrentRoom.Draw(Point.Zero);
            LoZGame.Instance.SpriteBatch.End();

            LoZGame.Instance.SpriteBatch.Begin(SpriteSortMode.FrontToBack, BlendState.NonPremultiplied, SamplerState.PointClamp, DepthStencilState.DepthRead, RasterizerState.CullNone, LoZGame.Instance.BetterTinting);
            LoZGame.Instance.GameObjects.Draw();
            LoZGame.Instance.SpriteBatch.End();

            LoZGame.Instance.SpriteBatch.Begin(SpriteSortMode.FrontToBack, BlendState.NonPremultiplied, SamplerState.PointClamp, DepthStencilState.DepthRead, RasterizerState.CullNone);
            LoZGame.Instance.GameObjects.Enemies.Draw();
            LoZGame.Instance.GameObjects.Entities.Draw();
            InventoryComponents.Instance.DrawText();
            foreach (IPlayer player in LoZGame.Instance.Players)
            {
                player.Draw();
            }
            LoZGame.Instance.SpriteBatch.End();

            LoZGame.Instance.SpriteBatch.Begin(SpriteSortMode.FrontToBack, BlendState.NonPremultiplied, SamplerState.PointClamp, DepthStencilState.DepthRead, RasterizerState.CullNone);
            InventoryComponents.Instance.DrawInventoryElements();
            LoZGame.Instance.SpriteBatch.End();
        }
    }
}