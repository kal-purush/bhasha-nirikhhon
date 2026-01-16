using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Assets.scripts
{
    class PlugEnds
    {
        public Coordinate first;
        public Coordinate second;
        public PlugEnds()
        {
            first = new Coordinate();
            second = new Coordinate();
        }

        public PlugEnds(int x1, int y1, int x2, int y2)
        {
            first = new Coordinate(x1, y1);
            second = new Coordinate(x2, y2);
        }

        public override bool Equals(Object obj)
        {
            PlugEnds plugCoord = obj as PlugEnds;
            if (plugCoord == null)
            {
                return false;
            }
            //Annoying, but we could be returning these coordinates in the reverse order.
            var returnValue = (plugCoord.first.Equals(first) &&
                                plugCoord.second.Equals(second)) ||
                                (plugCoord.first.Equals(second) &&
                                plugCoord.second.Equals(first));
            return returnValue;
        }

        public override int GetHashCode()
        {
            return base.GetHashCode();
        }
    }
}