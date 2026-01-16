using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Class_VlolumePiramide
{
    public class Piramide
    {
        private int _S; // S - площадь основания
        private int _H; // H - высота 

        public int S
        {
            get
            {
                return _S;
            }
            set
            {
                if (value < 0)
                    throw new ArgumentException();
                 _S = value;
            }
        }

        public int H
        {
            get
            {
                return _H;
            }
            set
            {
                if (value < 0)
                    throw new ArgumentException();
                _H = value;
            }
        }
        public double CalculateVolume()
        {
            return (1 / 3) * S * H;
        }


    }
}