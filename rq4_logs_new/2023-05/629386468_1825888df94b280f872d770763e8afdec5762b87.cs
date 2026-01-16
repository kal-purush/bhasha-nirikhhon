using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;


namespace hogward.Classes
{
    public class train
    {
        public static string Train()
        {
            int min = 5*60;
            int[,] DepartureTime = new int[4,2];
            DepartureTime[0,0] = 08;
            DepartureTime[0, 1] = 00;
            DepartureTime[1, 0] = 12;
            DepartureTime[1, 1] = 00;
            DepartureTime[2, 0] = 16;
            DepartureTime[2, 1] = 00;
            DepartureTime[3, 0] = 20;
            DepartureTime[3, 1] = 00;
            DateTime dt = DateTime.Now;
            int  TimeHour =Convert.ToInt16(dt.ToString("HH"));
            int TimeMin = Convert.ToInt16(dt.ToString("mm"));
            int RHour = 0, RMin = 0;
            for (int i = 0;i<4;i++)
            {
                if (TimeHour*60 + TimeMin > 20*60)
                {
                    return "Next Tomarow 8:00";
                    
                }
                if (DepartureTime[i, 0] > TimeHour)
                {
                    if (DepartureTime[i, 1] < TimeMin)
                    {
                        RMin = 60 - TimeMin;
                        RHour = DepartureTime[i, 0] - TimeHour - 1;
                    }
                    else
                    {
                        RHour = DepartureTime[i, 0] - TimeHour;
                    }
                }
                else
                    continue;
                if (RMin + RHour*60 <min)
                {
                    min = RMin + RHour * 60;
                }
            }
            return Convert.ToString(min) + " Left to train start to move";
        }
    }
}