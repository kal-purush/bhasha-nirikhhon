using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace ChessAI_Bck
{
    internal class PGNLog
    {
        public string ID { get; set; }
        public string Date { get; set; }
        public string White { get; set; }

        public string Black { get; set; }

        public string Result { get; set; }

        public int WhiteELO { get; set; }

        public int BlackELO { get; set; }

        public string TimeControl { get; set; }

        public string Termination { get; set; }

        public string PGN { get; set; }
    }
}