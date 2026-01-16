using UnityEngine;

public class PrefabBossSpikeTrap : Trap
{
    [SerializeField] private int trapDamage = default;

    //===========================================================================
    protected override void Update()
    {
        base.Update();

        if (currentAnimationIndex == trapSprites.Length)
            Destroy(gameObject, 1.0f);
    }

    protected override void TriggerTrapEffect()
    {
        if (playerInside)
            Player.Instance.GetComponent<PlayerHeart>().UpdateCurrentHeart(-trapDamage);
    }
}