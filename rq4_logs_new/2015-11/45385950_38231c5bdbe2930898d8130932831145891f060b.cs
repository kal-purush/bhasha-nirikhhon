using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace AntWars.Config
{
    class GameConfig
    {
        public int sugarMin { get; set; }
        public int sugarMax { get; set; }
        public int sugarAmountMin { get; set; }
        public int sugarAmountMax { get; set; }
        public int startAntAmount { get; set; }
        public int boardWidth { get; set; }
        public int boardHeigth { get; set; }
        public int startMoney { get; set; }
        //TODO Gewinnbedingungen
    }
}