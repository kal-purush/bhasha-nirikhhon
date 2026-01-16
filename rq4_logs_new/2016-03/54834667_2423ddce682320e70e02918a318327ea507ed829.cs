using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Class_VolumeParalepipid
{
   
    public class Parallelepiped
    {
        private int _A; // A - ширина
        private int _B; // B - длинна
        private int _C; // С - высота 
        
        public int A
        {
            get
            {
                return _A;
            }
            set
            {
                if (value < 0)
                    throw new ArgumentException();
                _A = value;
            }
        }

        public int B
        {
            get
            {
                return _B;
            }
            set
            {
                if (value < 0)
                    throw new ArgumentException();
                _B = value;
            }
        }
        
        
        public int C
        {
            get
            {
                return _C;
            }
            set
            {
                if (value < 0)
                    throw new ArgumentException();
                _C = value;
            }
        } 
            
        public int CalculateVolume()
        {
            return A * B * C;
        }


    }



}

