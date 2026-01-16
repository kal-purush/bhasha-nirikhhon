using System;
using System.Collections.Generic;
using UnityEngine;

public class PlayerAbilitiesApplier : MonoBehaviour
{
    private CreatureBody _creatureBody;
    private MoveController _moveController;

    enum AbilityType
    {
        NONE
    }
    
    struct Ability
    {
        public AbilityType type;
        public float strength;
    }

    private void Awake()
    {
        _creatureBody = GetComponent<CreatureBody>();
        _moveController = GetComponent<MoveController>();

        var abilities = GetAbilities();

        foreach (var ability in abilities)
        {
            switch (ability.type)
            {
                case AbilityType.NONE:
                    break;
                default:
                    throw new NotImplementedException();
            }
        }
    }
    
    private List<Ability> GetAbilities()
    {
        throw new NotImplementedException();
    }
}