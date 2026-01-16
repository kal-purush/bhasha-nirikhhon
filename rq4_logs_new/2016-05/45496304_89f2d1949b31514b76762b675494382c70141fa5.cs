using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Assets.scripts.Actor;
using Assets.scripts.AI;
using Assets.scripts.Skills.Base;
using Assets.scripts.Skills.SkillEffects;
using UnityEngine;
using Object = UnityEngine.Object;

namespace Assets.scripts.Skills.ActiveSkills
{
	public class CellJump : ActiveSkill
	{
		public int jumpSpeed = 50;

		public CellJump()
		{
			castTime = 0f;
			coolDown = 0f;
			reuse = 2f;
			updateFrequency = 0.1f;
			baseDamage = 10;
			resetMoveTarget = false; 

			range = 5;
			AvailableToPlayerAsAutoattack = true;
		}

		public override SkillId GetSkillId()
		{
			return SkillId.CellJump;
		}

		public override string GetVisibleName()
		{
			return "Cell Jump";
		}

		public override string GetDescription()
		{
			return "Autoattack that makes player jump forward in short distance.";
		}

		public override Skill Instantiate()
		{
			return new CellJump();
		}

		public override SkillEffect[] CreateEffects(int param)
		{
			return new SkillEffect[] {  };
		}

		public override void InitTraits()
		{
			AddTrait(SkillTraits.Jump);
		}

		public override bool OnCastStart()
		{
			if (castTime > 0)
				CreateCastingEffect(true, "SkillTemplate");
			return true;
		}

		public override void OnLaunch()
		{
			DeleteCastingEffect();

			UpdateMouseDirection();
			if (GetOwnerData().GetOwner().AI is PlayerAI)
				GetOwnerData().JumpForward(mouseDirection, range, jumpSpeed);
			else
			{
				GetOwnerData().JumpForward(GetOwnerData().GetForwardVector(), range, jumpSpeed);
			}
		}

		public override void OnFinish()
		{
		}

		public override void MonoUpdate(GameObject gameObject, bool fixedUpdate)
		{
		}

		public override void MonoStart(GameObject gameObject)
		{
		}

		public override void MonoDestroy(GameObject gameObject)
		{
		}

		public override void MonoTriggerEnter(GameObject gameObject, Collider2D coll)
		{
		}

		public override void OnAfterEnd()
		{
		}

		public override void OnAterReuse()
		{
		}

		public override void UpdateLaunched()
		{
		}

		public override void OnAbort()
		{
			GetOwnerData().AbortMeleeAttacking();
		}

		public override bool CanMove()
		{
			return true;
		}

		public override bool CanRotate()
		{
			return true;
		}
	}
}