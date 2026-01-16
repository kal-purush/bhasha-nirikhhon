using UnityEngine;

public class UpgradeItemHealthCandy : UpgradeItem
{
    [SerializeField] private int increaseCurrentHeart = default;

    //===========================================================================
    protected override void AddItemEffect()
    {
        Player.Instance.GetComponent<PlayerHeart>().UpdateCurrentHeart(increaseCurrentHeart);
    }

    protected override void RemoveItemEffect()
    {
        Player.Instance.GetComponent<PlayerHeart>().UpdateCurrentHeart(-increaseCurrentHeart);
    }
}