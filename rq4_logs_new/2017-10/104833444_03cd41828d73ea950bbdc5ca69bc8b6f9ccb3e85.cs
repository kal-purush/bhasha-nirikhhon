using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class SpinAttack : Move {
    public SpinAttack(string _name) : base(_name) { }

    override public Dictionary<string, int> processMove(Fighter attacker, Fighter target)
    {
        int damage = 4 + (int)(.5f * attacker.strength);
        attacker.spendStamina(4);
        return new Dictionary<string, int>
        {
            {Constants.HP, damage}
        };
    }

    public override bool moveEligible(Fighter attacker)
    {
        return attacker.stamina >= 4;
    }
}