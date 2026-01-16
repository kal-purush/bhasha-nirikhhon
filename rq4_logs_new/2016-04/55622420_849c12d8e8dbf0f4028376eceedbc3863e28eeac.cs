using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Zoolandia.Animals
{
    class FlyingSquirrel : Animal, IAmbulatory, IFlying
    {
        public double airSpeed
        {
            get
            {
                throw new NotImplementedException();
            }

            set
            {
                throw new NotImplementedException();
            }
        }

        public double groundSpeed
        {
            get
            {
                throw new NotImplementedException();
            }

            set
            {
                throw new NotImplementedException();
            }
        }

        public void fly()
        {
            throw new NotImplementedException();
        }

        public void walk()
        {
            throw new NotImplementedException();
        }
    }
}