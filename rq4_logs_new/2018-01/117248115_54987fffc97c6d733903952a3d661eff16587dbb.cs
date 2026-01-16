using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace TBGResearch.Classes
{
    public struct ResearchBonus
    {
        public string Name;
        public int BonusValue;
        public TechTreeType TreeTarget;
        public PartType PartTarget;
    }

    public struct EventBonus
    {
        public string FrameTag;
        public int TurnsToLast;
        public int BonusForAllLines;
        public int SingleLumpBonus;
    }
}