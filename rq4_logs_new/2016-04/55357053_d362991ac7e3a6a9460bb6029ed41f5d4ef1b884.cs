using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Automobilka
{
    public class NarrowWay
    {
        private double timeOfArrival;
        public NarrowWay()
        {
            timeOfArrival = 0;
        }
        // najdlhsi cas prichodu aktualnych aut na ceste
        public double realTime(double expectedTime)
        {
            if(expectedTime < timeOfArrival)
            {
                return timeOfArrival;
            }
            timeOfArrival = expectedTime;
            return expectedTime;
        }
    }
}