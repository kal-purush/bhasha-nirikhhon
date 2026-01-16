using UnityEngine;

public class OneTimeExperienceAwardEffect : OneTimeRelicEffect
{
    [SerializeField] private Vector2Int experienceAwardAmountRange;
    [SerializeField] private Vector2 experienceBoostFractionRange;
    [SerializeField] private EntityData expDrop;
    private bool used;
    private float experienceBoostFraction;
    private int experienceAwardAmount;

    public override void ObtainedEffect()
    {
        experienceAwardAmount = Random.Range(experienceAwardAmountRange.x, experienceAwardAmountRange.y);
        experienceBoostFraction = Random.Range(experienceBoostFractionRange.x, experienceBoostFractionRange.y);
        if (experienceBoostFraction > 0)
        {
            EventStore.Instance.PublishExpBoostFraction(experienceBoostFraction);
        }
    }

    public override void UsedEffect()
    {
        if (used) return;
        used = true;
        var expEntity = new ObtainedEntity(expDrop, experienceAwardAmount);
        EventStore.Instance.PublishEntityObtainedClick(expEntity);
    }

    public override string GetDescription()
    {
        var res = "";
        if (experienceAwardAmount > 0)
        {
            res += $"Gives player {experienceAwardAmount} exp\n";
        }

        if (experienceBoostFraction > 0)
        {
            res += $"Player receives {experienceBoostFraction * 100:N0}% more exp\n";
        }

        return res;
    }
}