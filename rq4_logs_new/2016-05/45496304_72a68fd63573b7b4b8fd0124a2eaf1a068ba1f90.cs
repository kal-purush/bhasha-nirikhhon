using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Assets.scripts.Actor;
using UnityEngine;

namespace Assets.scripts.Skills.SkillEffects
{
	public class EffectHealSelf : SkillEffect
	{
		private int value;
		private float mul;

		private float temp;

		public EffectHealSelf(int value)
		{
			this.value = value;
			mul = 0f;
		}

		public EffectHealSelf(float mul)
		{
			value = 0;
			this.mul = mul;
		}

		public override void ApplyEffect(Character source, GameObject target)
		{
			if (target == null)
				return;

			if (value > 0)
				source.ReceiveHeal(Source, value);

			int amount = (int)(source.Status.MaxHp * mul);
			if (amount > 0)
				source.ReceiveHeal(Source, amount);
		}

		public override void Update()
		{
		}

		public override void OnRemove()
		{
		}
	}
}