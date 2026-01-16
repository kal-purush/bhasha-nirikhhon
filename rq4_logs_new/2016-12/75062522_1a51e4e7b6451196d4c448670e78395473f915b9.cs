using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Footap
{
    class Gaest : Hus
    {
        public Gaest(int husNr) : base(husNr)
        {
        }

        public int Barn { get; set; }
        public int Ung { get; set; }
        public int Voksen { get; set; }

        public Gaest(int Barn, int Ung, int Voksen, int husNr)
        {
            Barn = Barn;
            Ung = Ung;
            Voksen = Voksen;
            husNr = husNr;
        }
        
    }
    
}