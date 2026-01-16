using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Fido2016
{
    class Anlegeposition
    {
        int _posx;
        int _posy;
        int _punktzahl;

        public Anlegeposition()
        {

        }

        public Anlegeposition(int x, int y, int p)
        {
            posx = x;
            posy = y;
            punktzahl = p;
        }

        public int posx
        {
            get { return _posx; }
            set { _posx = value; }
        }

        public int posy
        {
            get { return _posy; }
            set { _posy = value; }
        }

        public int punktzahl
        {
            get { return _punktzahl; }
            set { _punktzahl = value; }
        }
    }
}