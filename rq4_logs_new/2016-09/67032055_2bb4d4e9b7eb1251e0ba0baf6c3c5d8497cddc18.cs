using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using NemesesGame;

namespace NemesesGame
{
    public enum ArmyType { Regulars }
    
    public class Army
    {
        public Army ()
        {
            //init army_ dict
            army_.Add(ArmyType.Regulars, 0);
        }
        
        Dictionary<ArmyType, int> army_ = new Dictionary<ArmyType, int>();

        public int Regulars { get { return army_[ArmyType.Regulars]; } set { army_[ArmyType.Regulars] = value; } }

        public Dictionary<ArmyType, int> Army_ { get { return army_; } set { army_ = value; } }
    }
}