using Microsoft.Xna.Framework;
using Microsoft.Xna.Framework.Graphics;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace SeriousGame
{
	class Platform
	{
		private Vector2 _platformPosition;
		private Vector2 _platformSize;
		private Texture2D _platformTexture;

		public Platform (Vector2 pos, Vector2 size) 
		{
			_platformPosition = pos;
			_platformSize = size;
			_platformTexture = TextureManager.Instance.Platform;
		}

		public Rectangle boundingBox {
			get {
				Rectangle rect = new Rectangle ((int)_platformPosition.X, (int)_platformPosition.Y, (int)_platformSize.X, (int)_platformSize.Y);;
				return rect;
			}
		}

		public void Update(GameTime gameTime, int offset)
		{
			
		}

		public void Draw(SpriteBatch spriteBatch, int offset)
		{
			spriteBatch.Draw(_platformTexture, new Vector2(10, 650 + offset), Color.Green);
		}
	}
}