using System;
using System.Collections;
using System.Collections.Generic;
using UnityEngine;

namespace MastersOfTempest.PlayerControls.Spellcasting
{
    public class SteerHardLeftSpell : Spell
    {
        public override Charge[] SpellSequence
        {
            get
            {
                return new Charge[] { Charge.Fire, Charge.Wind, Charge.Fire, Charge.Wind };
            }
        }

        public override String Name
        {
            get
            {
                return "Right wind";
            }
        }

        public override PlayerAction GetPlayerAction()
        {
            return new SteerShip(SteerShip.SteeringDirection.HardLeft, newSpellCast);
        }

        public override Color SpellColor
        {
            get
            {
                return new Color(255 / 255f, 144f / 255f, 255 / 255f);
            }
        }
    }
}