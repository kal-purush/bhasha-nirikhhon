using System;
using System.Collections;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Text;
using Assets.scripts.Actor;
using Assets.scripts.Skills;
using Assets.scripts.Skills.Base;
using UnityEngine;
using Debug = UnityEngine.Debug;
using Random = UnityEngine.Random;

namespace Assets.scripts.AI
{
	public class SupportMonsterAI : MonsterAI
	{
        // how often does he think about evading; -1 to disable
	    public float evadeInterval = -1;

        // -1 to make it always 100% 
        public float evadeChance = -1;
        private float lastEvadeTime;

		// float around target
		public float floatChance = -1;
		public float floatInterval = -1;
		public float floatSpeed = 4;
		public int floatRange = 5;
		public float lastFloatTime;

		public bool shootWhileMoving = false;

		public SupportMonsterAI(Character o) : base(o)
		{
		}

		protected virtual bool IsLowHp(int hpPercent)
		{
			return (30 + UnityEngine.Random.Range(-10, 10) >= hpPercent);
		}

		private int lastAngle = 0;

		protected override void AttackTarget(Character target)
		{
			//SetMainTarget(target);

			bool isCasting = Owner.GetData().IsCasting;
			bool isMeleeAttacking = Owner.GetData().IsMeleeAttacking();
			//float distSqr = Utils.DistanceObjectsSqr(Owner.GetData().GetBody(), target.GetData().GetBody());
			float distSqr = Utils.DistanceSqr(Owner.GetData().GetBody().transform.position, target.GetData().GetBody().transform.position);
			int hpPercentage = (int)((GetStatus().Hp / (float)GetStatus().MaxHp) * 100);

			if (isCasting)
			{
				ActiveSkill castedSkill = null;

				foreach (ActiveSkill sk in Owner.Status.ActiveSkills)
				{
					if (sk.HasTrait(SkillTraits.BuffDamage) || sk.HasTrait(SkillTraits.BuffDefense))
					{
						castedSkill = sk;
						break;
					}
				}

				if (castedSkill != null)
				{
					GameObject initTarget = castedSkill.InitTarget;
					if (initTarget != null)
					{
						float dist = Utils.DistanceSqr(Owner.GetData().GetBody().transform.position, initTarget.transform.position);

						if (dist > Math.Pow(castedSkill.range, 2))
						{
							castedSkill.AbortCast();
						}
					}
				}
			}

			// already doing something
			if (isCasting || Owner.Status.IsStunned())
				return;

			if (Owner.GetData().Target == null || Owner.GetData().Target.Equals(target.GetData().GetBody()))
				Owner.GetData().Target = target.GetData().GetBody();

			/*if (IsLowHp(hpPercentage))
			{
				StartAction(RunAway(target, 5f, 20), 1f);
				return;
			}*/


			Collider2D[] colls = Physics2D.OverlapCircleAll(Owner.GetData().transform.position, AggressionRange*3);
			List<Character> targets = new List<Character>();

			foreach (Collider2D coll in colls)
			{
				if (coll.gameObject == null) continue;

				Character ch = Utils.GetCharacter(coll.gameObject);

				if (ch != null)
				{
					if (!Owner.CanAttack(ch) && !Owner.Equals(ch) && !ch.IsInteractable())
					{
						targets.Add(ch);
					}
				}
			}

			Character supportTarget = SelectSupportTarget(targets);

			if (supportTarget != null)
			{
				Debug.Log(supportTarget.Name);
				// try spawn skills
				List<Skill> supportSkills = GetAllSkillsWithTrait(SkillTraits.BuffDefense);
				foreach (Skill s in supportSkills)
				{
					if (s.CanUse())
					{
						Owner.GetData().Target = supportTarget.GetData().GetBody();

						if (StartAction(CastSkill(supportTarget, (ActiveSkill)s, Utils.DistanceSqr(supportTarget.GetData().transform.position, Owner.GetData().GetBody().transform.position), false, true, 0, 0), 0.5f))
							return;
					}
				}
			}

            if (evadeInterval > -1 && lastEvadeTime + evadeInterval < Time.time)
		    {
		        if (evadeChance < 0 || Random.Range(1, 100) < evadeChance)
		        {
		            Vector3 pos = Utils.GenerateRandomPositionAround(Owner.GetData().GetBody().transform.position, 4f, 3f);

		            if (StartAction(MoveAction(pos, true), 3f))
		            {
						Debug.DrawLine(pos, Owner.GetData().transform.position, Color.blue, 4f);
                        lastEvadeTime = Time.time;
                        return;
		            }
		        }
		    }

			if (distSqr < 10*10 && floatInterval > -1 && lastFloatTime + floatInterval < Time.time)
			{
				if (floatChance < 0 || Random.Range(1, 100) < floatChance)
				{
					// circulate target
					Vector3 pos = Utils.GenerateRandomPositionOnCircle(target.GetData().GetBody().transform.position, floatRange, lastAngle);

					if (StartAction(MoveAction(pos, true, floatSpeed), 2f, false))
					{
						lastAngle = lastAngle + Random.Range(-2, 2);
						Debug.DrawLine(pos, Owner.GetData().transform.position, Color.green, 4f);
						lastEvadeTime = Time.time;
						return;
					}
				}
			}

			if(HasMeleeSkill())
				Owner.GetData().MeleeInterract(target.GetData().GetBody(), true);
		}

		private Character SelectSupportTarget(List<Character> targets)
		{
			CompareByHpRatio comp = new CompareByHpRatio();
			targets.Sort(comp);

			if(targets.Count > 0)
				return targets[0];
			return null;
		}
	}

	public class CompareByHpRatio : Comparer<Character>
	{
		public override int Compare(Character x, Character y)
		{
			float ratioX = x.Status.Hp / (float)x.Status.MaxHp;
			float ratioY = y.Status.Hp / (float)y.Status.MaxHp;

			return ratioX.CompareTo(ratioY);
		}
	}
}