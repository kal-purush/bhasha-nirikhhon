using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Assets.scripts.Actor;
using Assets.scripts.Skills;
using Assets.scripts.Skills.Base;
using UnityEngine;

namespace Assets.scripts.AI.Modules
{
	public class CoverAllyModule : AIAttackModule
	{
		public CoverAllyModule(MonsterAI ai) : base(ai)
		{
		}

		public override void Init()
		{
			canTrigger = true;
		}

		public override bool Trigger(Character target, float distSqr)
		{
			Character closestAlly = null;

			if (ai.HasMaster())
			{
				closestAlly = ai.GetMaster();
			}
			//TODO select another mob to coveR?
			/*else if (target != null)
			{
				closestAlly = target;
			}*/

			if(closestAlly != null)
			{
				Character masterTarget = closestAlly.AI.GetMainTarget();

				if (masterTarget == null)
					return false;

				Vector3 masterTargetPos = masterTarget.GetData().GetBody().transform.position;
				Vector3 targetPos = Vector3.Lerp(closestAlly.GetData().transform.position, masterTargetPos, 0.5f);

				ai.MoveTo(targetPos, false);
				return true;
			}

			return false;
		}
	}
}