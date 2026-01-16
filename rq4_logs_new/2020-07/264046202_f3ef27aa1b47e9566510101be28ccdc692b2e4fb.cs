using System;
using System.Collections.Generic;
using System.Linq;
using System.Security.Cryptography.X509Certificates;
using System.Text;
using System.Threading.Tasks;

namespace ComboCounter.Classes
{
    class Tools
    {
        public static string FormatTimeSetter(int seconds)
        {
            return string.Format("{0:00}:{1:00}", seconds / 60, seconds % 60);
        }

        public static string FormatCurrentTime(long msecs)
        {
            long minutes = msecs / 1000 / 60;
            long seconds = msecs / 1000 % 60;
            long fracSecs = msecs % 1000 / 100;
            return string.Format("{0:00}:{1:00}.{2:0}", minutes, seconds, fracSecs);
        }
    }
}