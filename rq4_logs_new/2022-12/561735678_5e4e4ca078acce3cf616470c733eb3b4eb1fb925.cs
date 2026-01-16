using UnityEngine;

public class PickablePowerUp : AbstractWeapon
{
    public float duration = 5.0f;
    public float effectStrength = -1.0f;
    public CreatureEffectManager.EffectType effectType = CreatureEffectManager.EffectType.Damage;
    
    public override bool Activate(Vector2 direction)
    {
        GetComponentInParent<CreatureEffectManager>()
            .AddEffect(new CreatureEffectManager.Effect(
                duration, 
                effectStrength, 
                effectType));
        return true;
    }
}