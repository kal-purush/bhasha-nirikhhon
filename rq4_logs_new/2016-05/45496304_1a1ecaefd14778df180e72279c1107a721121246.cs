using System.Collections.Generic;
using System.Linq;
using System.Text;
using Assets.scripts.Actor;
using UnityEngine;
using Random = System.Random;

namespace Assets.scripts.AI.Modules
{
	public abstract class AIAttackModule
	{
		protected bool available;
		protected float interval;
		protected float chance;
		protected float lastActionTime;

		protected MonsterAI ai;
		public AIAttackModule(MonsterAI ai)
		{
			this.ai = ai;

			available = false;
			interval = -1;
			chance = -1;
		}

		public abstract void Init();

		public bool Launch(Character target, float distSqr)
		{
			if (CanTrigger())
			{
				if (Trigger(target, distSqr))
				{
					lastActionTime = Time.time;
					return true;
				}
			}

			return false;
		}

		/// <returns>true if launched, false if not launched</returns>
		public abstract bool Trigger(Character target, float distSqr);

		public bool CanTrigger()
		{
			if (!available)
				return false;

			if (interval < 0 || lastActionTime + interval < Time.time)
			{
				if (chance < 0 || UnityEngine.Random.Range(1, 100) < chance)
					return true;
			}

			return false;
		}

		public int GetHpPercentage()
		{
			return (int)((ai.GetStatus().Hp / (float)ai.GetStatus().MaxHp) * 100);
		}
	}
}