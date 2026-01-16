using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace SimSharp.Samples
{
    class priority
    {
        private static priority instance = null;
        public static priority getInstance()
        {
            
            if (instance == null)
            {
                instance = new priority();
            }
            return instance;
        }
        public int getPriority(DateTime TTL)//changes of this method, has to be also changed in TTL Method of Person_Gen
        {
            int priority;
            int TTLInSec = TTL.Hour * 3600 + TTL.Minute * 60 + TTL.Second;
            if (TTLInSec <= 0) priority = 3; //dead
            else if (TTLInSec > 0 && TTLInSec <= 3600) priority = 0; //TTL <= 60min -> hopeless
            else if (TTLInSec > 3600 && TTLInSec <= 36000) priority = 1; //TTL > 15min and <= 10h -> serverely injured
            else priority = 2; //TTL > 10h -> slightly injured
            return priority;

        }
    }
}