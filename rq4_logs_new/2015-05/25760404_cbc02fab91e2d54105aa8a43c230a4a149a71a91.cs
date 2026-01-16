using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Microsoft.Xna.Framework;
using Microsoft.Xna.Framework.Graphics;
using SeaBattle.Common.GameEvent;
using SeaBattle.Common.Session;
using SeaBattle.Common.Utils;
using SeaBattle.NetWork;
using SeaBattle.View;

namespace SeaBattle.Game
{
    public class GameModel
    {
        public Camera2D Camera2D { get; private set; }

        // explosions -> exploded time
        // private readonly Dictionary<DrawableGameObject, long> _explosions;

        private int _updateCouter;

        private readonly TimeHelper _timeHelper;

        /// <summary>
        /// Все GameEvent'ы с момента последнего синхрокадра,
        /// нужно хранить их! 
        /// </summary>
        private readonly List<AGameEvent> _serverGameEvents = new List<AGameEvent>();

        private readonly GameLevel _gameLevel;

        private DrawableGameObject[] _drawableGameObjects;

        public GameModel(GameLevel gameLevel, TimeHelper timeHelper)
        {
            _gameLevel = gameLevel;
            _timeHelper = timeHelper;

            Camera2D = new Camera2D(gameLevel.Width, gameLevel.Height);

        }
        
        /// <summary>
        /// Обновление позиций игровых объектов
        /// </summary>
        public void Update(GameTime gameTime)
        {
            
        }
        /*
        public void Draw(SpriteBatch spriteBatch)
        {
            var me = GetGameObject(Guid.NewGuid());

            Vector2 myPosition = me.Coordinates;

            Camera2D.Position = myPosition;

            spriteBatch.Begin(SpriteSortMode.BackToFront,
                              BlendState.AlphaBlend,
                              null,
                              null,
                              null,
                              null,
                              Camera2D.GetTransformation(Textures.GraphicsDevice));

            // draw background
            //_gameLevel.Draw(spriteBatch);

            // draw game objects
            foreach (DrawableGameObject drawableGameObject in _drawableGameObjects)
            {
                drawableGameObject.Draw(spriteBatch);
            }

            spriteBatch.End();
        }

        public DrawableGameObject GetGameObject(Guid id)
        {
            return _drawableGameObjects.First(x => x.Id == id);
        }*/
    }
}