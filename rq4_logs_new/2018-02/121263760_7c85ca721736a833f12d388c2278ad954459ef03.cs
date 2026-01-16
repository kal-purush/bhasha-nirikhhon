using System;
using Microsoft.Xna.Framework;
using Microsoft.Xna.Framework.Graphics;
using OneRoomRPGJam.Entities;

namespace OneRoomRPGJam.Entities.PlayerStates
{
	public class PlayerAttackingState : PlayerState
	{
		//TODO Create hitbox for sword that updates its X and Y position based on the player's
		//Width and height of hitbox should be the same. 

		public PlayerAttackingState(Player player) : base(player)
		{
			
		}

		public override void Init()
		{
		}

		public override void OnEnter()
		{
		}

		public override void OnExit()
		{
		}

		public override void Render(SpriteBatch spriteBatch)
		{
		}

		public override void Update(GameTime gameTime)
		{
		}
	}
}