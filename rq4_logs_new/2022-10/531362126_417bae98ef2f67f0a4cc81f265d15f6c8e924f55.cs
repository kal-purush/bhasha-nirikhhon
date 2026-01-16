using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace SampleCode05
{
	public enum CharacterJobs
	{
		Knight,
		Warrior,
		Mage,
		Priest,
	}

	public abstract class Character
	{
		public float HP
		{ get; private set; }
		public float MaxHP
		{ get; private set; }
		public float AttackPoint
		{ get; private set; }
		public float DefencePoint
		{ get; private set; }

		public CharacterJobs Job
		{ get; private set; }


		public Character(float maxHP, float attackPoint, float defencePoint,
			CharacterJobs job)
		{
			this.MaxHP = maxHP;
			this.HP = maxHP;
			this.AttackPoint = attackPoint;
			this.DefencePoint = defencePoint;
			this.Job = job;
		}

		public abstract void WriteProfile();


		public float Attack(Character target)
		{
			//ダメージ計算
			float damage = Functions.CalcDamage(this, target);

			//相手のHPを減らす
			target.HP -= damage;
			return damage;
		}
	}
}