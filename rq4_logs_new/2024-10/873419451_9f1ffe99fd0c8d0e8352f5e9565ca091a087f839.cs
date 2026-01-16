using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class EffectBase : BaseObject
{
    public Creature Owner;
    public Data.EffectData EffectData;
	public Define.EEffectType EffectType;

    protected float Remains { get; set; } = 0;
	protected Define.EEffectSpawnType _spawnType;
	protected bool Loop { get; set; } = true;

    public override bool Init()
	{
		if (base.Init() == false)
			return false;

		return true;
	}

    public virtual void SetInfo(int templateID, Creature owner, Define.EEffectSpawnType spawnType)
	{
		DataTemplateID = templateID;
		EffectData = Managers.Data.EffectDic[templateID];

		Owner = owner;
		_spawnType = spawnType;

		if (string.IsNullOrEmpty(EffectData.SkeletonDataID) == false)
			SetSpineAnimation(EffectData.SkeletonDataID, SortingLayers.SKILL_EFFECT);

		EffectType = EffectData.EffectType;
	}

	public virtual void ApplyEffect()
	{
		ShowEffect();
		StartCoroutine(CoStartTimer());
	}

    protected virtual void ShowEffect()
	{
		if (SkeletonAnim != null && SkeletonAnim.skeletonDataAsset != null)
			PlayAnimation(0, AnimName.IDLE, Loop);
	}

    protected void AddModifier(CreatureStat stat, object source, int order = 0)
	{
		if (EffectData.Amount != 0)
		{
			StatModifier add = new StatModifier(EffectData.Amount, Define.EStatModType.Add, order, source);
			stat.AddModifier(add);
		}

		if (EffectData.PercentAdd != 0)
		{
			StatModifier percentAdd = new StatModifier(EffectData.PercentAdd, Define.EStatModType.PercentAdd, order, source);
			stat.AddModifier(percentAdd);
		}

		if (EffectData.PercentMult != 0)
		{
			StatModifier percentMult = new StatModifier(EffectData.PercentMult, Define.EStatModType.PercentMult, order, source);
			stat.AddModifier(percentMult);
		}
	}

    protected void RemoveModifier(CreatureStat stat, object source)
	{
		stat.ClearModifiersFromSource(source);
	}

    public virtual bool ClearEffect(Define.EEffectClearType clearType)
	{
		Debug.Log($"ClearEffect - {gameObject.name} {EffectData.ClassName} -> {clearType}");

		switch (clearType)
		{
			case Define.EEffectClearType.TimeOut:
			case Define.EEffectClearType.TriggerOutAoE:
			case Define.EEffectClearType.EndOfAirborne:
				Managers.Object.Despawn(this);
				return true;

			case Define.EEffectClearType.ClearSkill:
				break;
		}

		return false;
	}

    protected virtual void ProcessDot()
	{

	}

    protected virtual IEnumerator CoStartTimer()
	{
		float sumTime = 0f;

		ProcessDot();

		while (Remains > 0)
		{
			Remains -= Time.deltaTime;
			sumTime += Time.deltaTime;

			// 틱마다 ProcessDotTick 호출
			if (sumTime >= EffectData.TickTime)
			{
				ProcessDot();
				sumTime -= EffectData.TickTime;
			}

			yield return null;
		}

		Remains = 0;
		ClearEffect(Define.EEffectClearType.TimeOut);
	}
}