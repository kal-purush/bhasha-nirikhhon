using System.Collections;
using System.Collections.Generic;
using UnityEngine;
using System;

public abstract class Action
{
    // the name that we use within the coding program
    protected string name;

    // the name that the user sees in the UI
    protected string alias;

    protected string description;
    protected TargetType targetType;

    // getters for the above properties
    public string Name { get => name; }
    public string Alias { get => alias; }
    public string Description { get => description; }
    public TargetType TargetType { get => targetType; }

    public Action(string name, string description, 
        string targetType, string alias)
    {
        // cast the string of targetType to enum form
        TargetType enumTargetType;
        try
        {
            enumTargetType = (TargetType)Enum.Parse(typeof(TargetType), targetType);
        }
        catch
        {
            Debug.Log($"Warning: {targetType} not recognised");
            enumTargetType = TargetType.ENEMY_SINGLE;
        }

        this.name = name;
        this.description = description;
        this.targetType = enumTargetType;
        this.alias = alias;
    }


    // perform an action for the item
    // user is the person casting it
    // targets are the targets of the skill
    public abstract IEnumerator performAction(Character user, Character[] targets);

    //calculates damage after defense formula
    public static int calcDamage(double damage, int defense)
    {
        return (int)(damage * (50d / (50d + defense)));
    }
}