using System;
using Microsoft.Xna.Framework;
using Microsoft.Xna.Framework.Graphics;

namespace OneRoomRPGJam
{
	//#TODO Derive CollisionEntity 
	//#TODO Derive PlayerEntity from CollisionEntity
	//#TODO Derive EnemyEntity from CollisionEntity
	//#TODO Derive ObjectEntity from CollisionEntity

	public abstract class Entity
	{
		protected int x;
		protected int y;
		protected int height;
		protected int width;
		protected Rectangle bounds;
		protected Texture2D texture; 

		public Rectangle getBounds()
		{
			bounds = new Rectangle(x,y,width,height);
			return bounds; 
		}
		public Entity()
		{
			
		}
		public virtual void Init()
		{
		}
		public virtual void Update(GameTime gameTime)
		{
		}
		public virtual void Render(SpriteBatch spriteBatch)
		{
			spriteBatch.Draw(texture, getBounds(), Color.White);
		}
	}
}