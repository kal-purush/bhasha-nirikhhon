using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class EnergyPotion : Item {
    
    public EnergyPotion()
    {
        id = spriteId = 0;
        effectTimer = 0;
        effectDuration = 3;
        name = "Energy Potion";
    }

    public override void onConsume(Player player) {
        player.character.setModifier(Skill.ENDURANCE, 5);
    }

    public override void onExpiration(Player player)
    {
        player.character.setModifier(Skill.ENDURANCE, 0);
    }

}