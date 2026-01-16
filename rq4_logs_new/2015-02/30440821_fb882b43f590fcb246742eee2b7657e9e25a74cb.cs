using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace RkoOuttaNowhere.Gameplay
{
    public enum Upgrades
    {
        None = 0
    }

    class GameManager
    {
        public int Currency;
        public int Health;
        public Upgrades CurrentUpgrades;
        
        //TODO:Other Stats here

        public int Waves_Current;
        public int Waves_Total;

        //TODO:Use Reference to Actual Level
        public int Level_Current;


    }
}