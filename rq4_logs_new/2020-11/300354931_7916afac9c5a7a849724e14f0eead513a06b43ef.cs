using System.Collections;
using System.Collections.Generic;
using System;
using UnityEngine;

// heal the player
public class ReviveItem : Item
{
    public ReviveItem(string name, string description,
        string targetType, string alias,
        int cost, float amountPercent)
        : base(name, description, targetType, alias, cost, amountPercent) { }

    override public IEnumerator performAction(Character user, Character[] targets)
    {
        if (targetType == TargetType.ALLY_SINGLE)
        {
            Character target = targets[0];
            int amount = (int)(target.health * amountPercent);
            target.healHealth(amount);
        }

        RemoveItemFromInventory();
        yield return new WaitForSeconds(.5f);
    }
}