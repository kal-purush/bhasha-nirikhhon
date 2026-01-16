using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Assets.scripts.Actor;
using Assets.scripts.Skills;
using Assets.scripts.Skills.ActiveSkills;
using Assets.scripts.Skills.Base;
using UnityEngine;
using Random = UnityEngine.Random;

namespace Assets.scripts.AI.Modules
{
	public class SpawnMinionModule : AIAttackModule
	{
		public delegate void SummonNotify(Monster m);

		public float spawnInterval = 5f;
		public float spawnIntervalRandomAdd = 1f;
		public int maxMinions = 20;

		private List<Monster> spawned = new List<Monster>();
		private List<Skill> spawnSkills;
		private float nextSpawnInterval;
		 
		public SpawnMinionModule(MonsterAI ai) : base(ai)
		{
			ai.UseTimers();
		}

		public override void Init()
		{
			spawnSkills = ai.GetAllSkillsWithTrait(SkillTraits.SpawnMinion);
			canTrigger = spawnSkills.Count > 0;

			InitNextSpawn();
		}

		private void InitNextSpawn()
		{
			nextSpawnInterval = spawnInterval + Random.Range(-spawnIntervalRandomAdd, spawnIntervalRandomAdd);
		}

		public override bool Trigger(Character target, float distSqr)
		{
			ActiveSkill spawnSkill = null;

			foreach (Skill s in spawnSkills)
			{
				if (s.CanUse())
				{
					spawnSkill = s as ActiveSkill;
					break;
				}
			}

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

			if (spawnSkill == null)
				return false;

			if (spawnSkill is ISummonNotifyCallback)
			{
				if (((ISummonNotifyCallback) spawnSkill).GetCallback() == null)
				{
					((ISummonNotifyCallback)spawnSkill).SetCallback(NotifySummonSpawned);
				}
			}

			if (count < 20 && ai.GetTimer("spawn", nextSpawnInterval))
			{
				if (ai.StartAction(ai.CastSkill(null, spawnSkill, 0, true, false, 0f, 0f), 0.5f))
				{
					ai.SetTimer("spawn");
					InitNextSpawn();
					return true;
				}
			}

			return false;
		}

		public void NotifySummonSpawned(Monster m)
		{
			m.AI.AddAggro(GameSystem.Instance.CurrentPlayer, 10000);

			spawned.Add(m);
		}
	}
}