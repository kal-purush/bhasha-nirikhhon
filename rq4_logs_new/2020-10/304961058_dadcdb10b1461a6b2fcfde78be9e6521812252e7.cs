using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace JoliBateau
{
    class Vent
    {
        //permet de calculer la vitesse du vent en fonction de P(x, y) le point au milieu de P1 et P2 
        public static double GetWindSpeed(double x, double y)
        {
            if (Point.cas == 'a')
                return 50;
            else if (Point.cas == 'b')
                if (y > 150)
                    return 50;
                else return 20;
            else if (y > 150)
                return 50;
            else return 20;
        }
        public static double GetWindDirection(double x, double y)
        {
            if (Point.cas == 'a')
                return 30;
            else if (Point.cas == 'b')
                if (y > 150)
                    return 180;
                else return 90;
            else if (y > 150)
                return 170;
            else return 65;
        }
    }
}