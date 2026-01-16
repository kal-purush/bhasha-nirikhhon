using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class MoveTemplate : Move {

    public MoveTemplate(string _name) : base(_name) { }

    override public Dictionary<string, int> processMove(Fighter attacker, Fighter target)
    {
        switch (attacker.weapon)
        {
            case "Spoon":
                return new Dictionary<string, int>()
                {
                    {Constants.HP, 1 }
                };
            case "Rolling Pin":
                return new Dictionary<string, int>()
                {
                    {Constants.HP, 2 }
                };
            case "Frying Pan":
                return new Dictionary<string, int>()
                {
                    {Constants.HP, 4 }
                };
        }
        return null;
    }

    public override bool moveEligible(Fighter attacker)
    {
        return true;
    }
}