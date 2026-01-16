using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class Guard : Move {
    public Guard(string _name) : base(_name) { }

    override public Dictionary<string, int> processMove(Fighter attacker, Fighter target)
    {
        attacker.increaseDefense(attacker.defense);
        return new Dictionary<string, int>
        {
            {Constants.DefenseEffect, 2}
        };        
    }

    public override bool moveEligible(Fighter attacker)
    {
        return attacker.stamina - 3 >= 0;
    }
}