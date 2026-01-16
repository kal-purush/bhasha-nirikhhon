using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Assets.GameData
{
    public class BlockDirt : Entity, IBlock
    {
        public float BreakTime
        {
            get
            {
                return 5f;
            }
        }
    }
}