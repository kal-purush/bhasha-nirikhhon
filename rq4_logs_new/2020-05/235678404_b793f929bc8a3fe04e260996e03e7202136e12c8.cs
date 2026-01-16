namespace LoZClone
{
    using Microsoft.Xna.Framework;
    using Microsoft.Xna.Framework.Graphics;
    using System.IO;

    class ProfilesState : GameStateEssentials, IGameState
    {
        private ISprite ProfileScreen;
        private ISprite SelectorSprite;
        private string profile1Dungeon;
        private string profile2Dungeon;
        private string profile3Dungeon;

        public ProfilesState()
        {
            SoundFactory.Instance.PlayLobbyTune();
            ProfileScreen = ScreenSpriteFactory.Instance.ProfilesScreen();
            SelectorSprite = LinkSpriteFactory.Instance.CreateSpriteLinkDown(Link.LinkColor.Green);
            SelectorSprite.SetFrame(0);
            string[] profile1 = File.ReadAllLines("../../../../etc/profiles/Profile#1.txt");
            profile1Dungeon = profile1[0];
            string[] profile2 = File.ReadAllLines("../../../../etc/profiles/Profile#2.txt");
            profile2Dungeon = profile2[0];
            string[] profile3 = File.ReadAllLines("../../../../etc/profiles/Profile#3.txt");
            profile3Dungeon = profile3[0];
        }

        /// <inheritdoc></inheritdoc>
        public override void PlayGame()
        {
            SoundFactory.Instance.StopAll();
            SoundFactory.Instance.PlayDungeonSong();
            LoZGame.Instance.GameState = new PlayGameState();
        }

        /// <inheritdoc></inheritdoc>
        public override void TitleScreen()
        {
            LoZGame.Instance.GameState = new TitleScreenState();
        }

        /// <inheritdoc></inheritdoc>
        public override void Draw()
        {
            LoZGame.Instance.SpriteBatch.Begin(SpriteSortMode.FrontToBack, BlendState.NonPremultiplied, SamplerState.PointClamp, DepthStencilState.DepthRead, RasterizerState.CullNone, LoZGame.Instance.BetterTinting);
            ProfileScreen.Draw(new Vector2(0, 0), LoZGame.Instance.DefaultTint, 0.9f);
            switch (LoZGame.Instance.SelectedProfile)
            {
                case 1:
                    SelectorSprite.Draw(new Vector2(GameData.Instance.GameStateDataConstants.ProfilesSelectorX, GameData.Instance.GameStateDataConstants.Profiles1SelectorY), LoZGame.Instance.DefaultTint, 0.99f);
                    break;
                case 2:
                    SelectorSprite.Draw(new Vector2(GameData.Instance.GameStateDataConstants.ProfilesSelectorX, GameData.Instance.GameStateDataConstants.Profiles2SelectorY), LoZGame.Instance.DefaultTint, 0.99f);
                    break;
                case 3:
                    SelectorSprite.Draw(new Vector2(GameData.Instance.GameStateDataConstants.ProfilesSelectorX, GameData.Instance.GameStateDataConstants.Profiles3SelectorY), LoZGame.Instance.DefaultTint, 0.99f);
                    break;
                default:
                    SelectorSprite.Draw(new Vector2(GameData.Instance.GameStateDataConstants.ProfilesSelectorX, GameData.Instance.GameStateDataConstants.Profiles1SelectorY), LoZGame.Instance.DefaultTint, 0.99f);
                    break;
            }
            LoZGame.Instance.SpriteBatch.End();
            LoZGame.Instance.SpriteBatch.Begin(SpriteSortMode.FrontToBack, BlendState.NonPremultiplied, SamplerState.PointClamp, DepthStencilState.DepthRead, RasterizerState.CullNone, LoZGame.Instance.BetterTinting);
            LoZGame.Instance.SpriteBatch.DrawString(LoZGame.Instance.Font, profile1Dungeon, new Vector2(GameData.Instance.GameStateDataConstants.ProfilesDungeonTextX, GameData.Instance.GameStateDataConstants.Profiles1DungeonTextY), Color.White);
            LoZGame.Instance.SpriteBatch.DrawString(LoZGame.Instance.Font, profile2Dungeon, new Vector2(GameData.Instance.GameStateDataConstants.ProfilesDungeonTextX, GameData.Instance.GameStateDataConstants.Profiles2DungeonTextY), Color.White);
            LoZGame.Instance.SpriteBatch.DrawString(LoZGame.Instance.Font, profile3Dungeon, new Vector2(GameData.Instance.GameStateDataConstants.ProfilesDungeonTextX, GameData.Instance.GameStateDataConstants.Profiles3DungeonTextY), Color.White);
            LoZGame.Instance.SpriteBatch.End();
        }
    }
}