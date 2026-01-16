using System;
using Microsoft.Xna.Framework;
using Microsoft.Xna.Framework.Graphics;

namespace OneRoomRPGJam
{
	public class PickUp : CollisionEntity
	{
		public enum Effect {HEALTH, MANA, SCORE, EXPERIENCE}
		public enum Size {LARGE, MEDIUM, SMALL}
		Effect effect;
		Size size;

		public PickUp(Effect effect, Size size)
		{
			this.effect = effect;
			this.size = size; 
		}

		public override void Init()
		{
			//Init texture based on effect and size. 
		}

		public void OnPickUp(Player player)
		{
			//TODO Pass the 'size' and 'effect' member to a function called by 'player' and adjust values accordingly
		}

		public override void Render(SpriteBatch spriteBatch)
		{
			//Function renders texture based on it's 'size' member. 

			if (size == Size.SMALL)
			{
				spriteBatch.Draw(texture, new Vector2(x, y), Color.White);
			}
			else if (size == Size.MEDIUM)
			{
				spriteBatch.Draw(texture, new Rectangle(x, y, width * (int)1.5, height * (int)1.5), new Rectangle(x, y, width, height), Color.White);
			}
			else if (size == Size.LARGE)
			{
				spriteBatch.Draw(texture, new Rectangle(x, y, width * 2, height * 2), new Rectangle(x, y, width, height), Color.White);
			}
		}

		public override void Update(GameTime gameTime)
		{
			base.Update(gameTime);
		}
	}
}