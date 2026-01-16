using System;
using GXPEngine.Core;
namespace GXPEngine
{
	public class Enemy : Entity
	{
		private Rectangle _rectangle;
		private float distanceToPlayer;
		private Player _player;
		private float enemySpeed;
		public Enemy( string filename, int columns, int rows, Player player ) : base( filename, columns, rows,EntityType.Enemy )
		{
			_player = player;
			SetOrigin( width / 2f, height / 2f );
		}

		public new void Update()
		{
			distanceToPlayer = this.DistanceTo( _player );
			if (distanceToPlayer <= 500 && y - _player.y <= 128 && y - _player.y >-128)
			{
				followPlayer();
			}
		}

		private void followPlayer ()
		{
			if (x < _player.x)
				xSpeed += enemySpeed;
			else
				xSpeed += enemySpeed;
		}

		public float GetDistanceToPlayer { get { return distanceToPlayer; } }
	}
}
