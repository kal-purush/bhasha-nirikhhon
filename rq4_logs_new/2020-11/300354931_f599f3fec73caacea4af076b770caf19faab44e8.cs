using System.Collections;
using System.Collections.Generic;
using System;
using UnityEngine;

// destress the player
public class DestressingItem : Item
{
    public DestressingItem(string name, string description,
        string targetType, string alias,
        int cost, float amountPercent)
        : base(name, description, targetType, alias, cost, amountPercent) { }

    override public IEnumerator performAction(Character user, Character[] targets)
    {

        if (targetType == TargetType.ALLY_SINGLE)
        {
            Character target = targets[0];
            int playerStress = target.stress;
            int amount = (int)(playerStress * amountPercent);
            target.SetCharacterStress(playerStress - amount);
        }
        else if (targetType == TargetType.ALLY_ALL)
        {
            foreach (Character target in targets)
            {
                int playerStress = target.stress;
                int amount = (int)(playerStress * amountPercent);
                target.SetCharacterStress(playerStress - amount);
            }
        }
        RemoveItemFromInventory();
        yield return new WaitForSeconds(.5f);
    }

}