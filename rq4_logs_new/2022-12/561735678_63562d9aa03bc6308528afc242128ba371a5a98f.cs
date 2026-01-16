using System;
using System.Collections.Generic;
using UnityEngine;
using Random = UnityEngine.Random;

public class PlayerWeaponEnchanter : MonoBehaviour
{
    public List<CreatureEffectManager.Effect> addedEffects = new();

    public float additionalMeleeDamage = 0;
    public float additionalDistantDamage = 0;

    public float meleeDamageCoefficient = 1;
    public float distantDamageCoefficient = 1;
    public float effectTimeCoefficient = 1;
    public float effectStrengthCoefficient = 1;
    public float timeoutCoefficient = 1;
    
    public float vampirismCoefficient = 0;
    public float criticalDamageCoefficient = 2;

    public float criticalDamageProbability = 0; 

    private CreatureBody _creatureBody;
    
    private void Start()
    {
        _creatureBody = GetComponent<CreatureBody>();
    }

    public float EnchantDamage(float damage, bool melee)
    {
        if (melee)
        {
            damage += additionalMeleeDamage;
            damage *= meleeDamageCoefficient;
        }
        else
        {
            damage += additionalDistantDamage;
            damage *= distantDamageCoefficient;
        }

        if (damage > 0) {
            _creatureBody.Heal(damage * vampirismCoefficient);
        }

        if (criticalDamageProbability > 0 && Random.value < criticalDamageProbability)
        {
            damage *= criticalDamageCoefficient;
        }

        return damage;
    }

    public float EnchantTimeout(float timeout)
    {
        timeout *= timeoutCoefficient;
        return timeout;
    }

    public CreatureEffectManager.Effect EnchantEffect(CreatureEffectManager.Effect effect)
    {
        effect.EffectStrength *= effectStrengthCoefficient;
        effect.TimeLast *= effectTimeCoefficient;
        return effect;
    }

    public void AdditionalEnemyAttack(CreatureEffectManager enemy)
    {
        foreach (var effect in addedEffects)
        {
            enemy.AddEffect(effect);
        }
    }
}