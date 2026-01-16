using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Telemetry
{
    class TelemetryManager
    {
        public string CurrentGear(float gear)
        {
                string _Gear;
                switch (gear)
                {
                    case 0: _Gear = "R"; break;
                    case 1: _Gear = "N"; break;
                    case 2: _Gear = "1"; break;
                    case 3: _Gear = "2"; break;
                    case 4: _Gear = "3"; break;
                    case 5: _Gear = "4"; break;
                    case 6: _Gear = "5"; break;
                    case 7: _Gear = "6"; break;
                    case 8: _Gear = "7"; break;
                    case 9: _Gear = "8"; break;
                    default: _Gear = "E"; break;
                }
                return _Gear;
        }

    }
}