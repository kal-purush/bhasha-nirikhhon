using Rhyth.BTree;
using System.Collections.Generic;
using UnityEngine;

public class GotoHealthNode : BNodeAdapter
{
    public override int MaxNumberOfChildren => 0;

    [SerializeField] private HealthValue target;
    [SerializeField] private DistanceReference distanceReference;
    [SerializeField] private float distance;

    private float minDistanceFromTargetSquared;

    public override void InnerBeginn()
    {
        if (distanceReference == DistanceReference.UseDistanceInMeter)
        {
            minDistanceFromTargetSquared = distance * distance;
        }
        else
        {
            Weapon weapon = Brain.GetComponent<Enemy>()?.EquippedWeapon?.Weapon;
            if (weapon == null)
                throw new System.Exception("Enemy " + Brain.name + " does not have a weapon but uses a Node which requires a weapon!");
            minDistanceFromTargetSquared = Mathf.Pow(weapon.Range * distance, 2.0f);
        }
    }

    [System.Serializable]
    private enum DistanceReference
    {
        UseDistanceInMeter, FactorWeaponRange
    }

    public override void Update()
    {
        Health health;
        if (!target || !(health = target.Get()) || !health.Alive)
        {
            CurrentStatus = Status.Failure;
            return;
        }

        Vector2 direction = health.transform.position - Brain.transform.position;
        if (direction.sqrMagnitude < minDistanceFromTargetSquared)
        {
            CurrentStatus = Status.Success;
            return;
        }
        direction.Normalize();

        Mover.ConstantDirection = direction;
        Mover.ShouldMove = true;
    }

    protected override BNode InnerClone(Dictionary<Value, Value> originalValueForClonedValue)
    {
        GotoHealthNode ghn = CreateInstance<GotoHealthNode>();
        ghn.target = CloneValue(originalValueForClonedValue, target) as HealthValue;
        ghn.distanceReference = distanceReference;
        ghn.distance = distance;
        return ghn;
    }

    protected override void InnerReplaceValues(Dictionary<Value, Value> originalReplace)
    {
        if (originalReplace.TryGetValue(target, out Value gottenHealth))
            target = gottenHealth as HealthValue;
    }
}