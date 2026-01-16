using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Class_VolumeBall
{
    public class ball 
    {
        private int _radius; //rarius - раиус шара 

        public int radius
        {
            get
            {
                return _radius;
            }
            set
            {
                if (value < 0)
                    throw new ArgumentException();
                _radius = value;
            }
        }
        public double CalculateVolume()
        {
            return (4 / 3) * Math.PI * (Math.Pow(radius, 3));
        }

    }
}