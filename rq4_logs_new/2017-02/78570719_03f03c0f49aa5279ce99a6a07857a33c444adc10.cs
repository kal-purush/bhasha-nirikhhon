using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using TAMKShooter.Configs;
using UnityEngine;

namespace TAMKShooter
{
	public class EnemyUnit : UnitBase
	{
		public EnemyUnits EnemyUnits { get; private set; }

		public override int ProjectileLayer
		{
			get
			{
				return LayerMask.NameToLayer ( Config.EnemyProjectileLayerName );
			}
		}

		public void Init(EnemyUnits enemyUnits)
		{
			EnemyUnits = enemyUnits;
		}

		protected override void Die ()
		{
			// Handle dying properly. Instantiate explosion effect, play sound etc.
			gameObject.SetActive ( false );
			EnemyUnits.EnemyDied ( this );

			base.Die ();
		}
	}
}