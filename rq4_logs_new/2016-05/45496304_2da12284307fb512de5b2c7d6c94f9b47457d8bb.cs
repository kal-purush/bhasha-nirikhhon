using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Text;
using Assets.scripts.Actor;
using Assets.scripts.Skills;
using Assets.scripts.Skills.ActiveSkills;
using Assets.scripts.Skills.Base;
using UnityEngine;
using Debug = UnityEngine.Debug;
using Random = UnityEngine.Random;

namespace Assets.scripts.AI
{
	public class SummonerMonsterAI : MonsterAI
	{
		public float spawnMinInterval = 4f;
		public float spawnMaxInterval = 5f;

		private float nextSpawnInterval;

		public int maxMonstersSpawned = 20;

		protected ActiveSkill spawnSkill = null;

		private List<Monster> spawned = new List<Monster>();

		public SummonerMonsterAI(Character o)
			: base(o)
		{
			UseTimers();
		}

		public override void AnalyzeSkills()
		{
			spawnSkill = (ActiveSkill)GetSkillWithTrait(SkillTraits.SpawnMinion);

			nextSpawnInterval = Random.Range(spawnMinInterval, spawnMaxInterval);
		}

		/*protected override void ThinkActive()
		{
			base.ThinkActive();

			bool isCasting = Owner.GetData().IsCasting;
			if (isCasting)
				return;

			DoSpawn();
		}*/

		protected override void AttackTarget(Character target)
		{
			SetMainTarget(target);

			bool isCasting = Owner.GetData().IsCasting;

			// already doing something
			if (isCasting)
				return;

			if (Owner.GetData().Target == null || Owner.GetData().Target.Equals(target.GetData().GetBody()))
				Owner.GetData().Target = target.GetData().GetBody();

			Vector3 ownerPos = Owner.GetData().GetBody().transform.position;

			DoSpawn();

			Vector3 nextTarget = Utils.GenerateRandomPositionAround(ownerPos, 5f);
			StartAction(MoveAction(nextTarget, false), 1f);
		}

		private void DoSpawn()
		{
			int count = 0;

			for (int i = spawned.Count - 1; i >= 0; i--)
			{
				Monster m = spawned[i];

				if (m.Status.IsDead)
				{
					spawned.RemoveAt(i);
					continue;
				}

				count++;
			}

			if (count < 20 && spawnSkill != null && GetTimer("spawn", nextSpawnInterval))
			{
				if (StartAction(CastSkill(null, spawnSkill, 0, true, false, 0f, 0f), 0.5f))
				{
					SetTimer("spawn");

					return;
				}
			}
		}

		public void NotifySummonSpawned(Monster m)
		{
			m.AI.AddAggro(GameSystem.Instance.CurrentPlayer, 10000);

			spawned.Add(m);
		}
	}
}