using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace TextAdventureCS.Items
{
    class MagicStrawberry_s : Objects
    {
        public MagicStrawberry_s(string name, bool acquirable) : base(name, acquirable)
        {

        }

        protected override void Description()
        {
            Program.PrintLine(50, "There is a box in the middle of the room,  that must be the magic strawberry's ! or not? ");
        }
    }
}