using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace PiffTeam
{
    public class MyMath
    {
        private static double startY; //Kezdőérték f(0)
        private static double startTime; //Kezdeti időpont x(0)
        private static double endTime; //Meddig fusson a szimuláció 
        private static int diffType; //A differenciál egyenlet megadási típusa
        //0 - implicit
        //1 - explicit
        //2 - adaptív

        
        private static double step; //Lépésköz (deltaT)
        private static double[] xCoordinates; // A lépéseket tartalmazó tömb
        private static double[] yCoordinates; // Az y értékeket tartalmazó tömb

        public double Step
        {
            get
            {
                return step;
            }
            set
            {
                step = value;
                xCoordinates = GetStep(startTime, endTime, step);
            }
        }
        //A bemeneti függvény
        public delegate double Function(double x, double y);

        /// <summary>
        /// X értékeket tartalmazó tömböt ad vissza, mely startól-finishig interval lépésközzel tartalmazza az értékeket.
        /// </summary>
        /// <param name="start">Kezdőértéke</param>
        /// <param name="finish"></param>
        /// <param name="interval">Lépésköz</param>
        /// <returns></returns>
        static public double[] GetStep(double start, double finish, double interval)
        {
            if (finish < start) throw new ArgumentException("Finish nem lehet nagyobb mint a start");
            if (interval <= 0) throw new ArgumentException("Az lépésköznek nagyobbnak kell lennie mint 0");

            double[] xCoordinates = new double[(int)((finish - start) / interval) + 1];
            int digits = 5;
            interval = Math.Round(interval, 5);

            //double actual = start;
            xCoordinates[0] = start;
            for (int i = 0; i < xCoordinates.Length; ++i)
            {
                xCoordinates[i] = interval * i + start;
                xCoordinates[i] = Math.Round(xCoordinates[i], digits);

            }
            xCoordinates[xCoordinates.Length - 1] = finish;
            return xCoordinates;
        } //Lépések kiszámolása
        public MyMath(double _starttime, double _endtime, double _starty, double _step, Function f)
        {
            startY = _starty;
            startTime = _starttime;
            endTime = _endtime;
            step = _step;
            xCoordinates = GetStep(startTime, endTime, step);
        }

        //runge kutta 4th method
        public void runge(Function f)
        {
            double w, k1, k2, k3, k4;
            yCoordinates = new double[xCoordinates.Length];
            w = startY;
            for (int i = 0; i < xCoordinates.Length; i++)
            {
                k1 = step * f(xCoordinates[i], w);
                k2 = step * f(xCoordinates[i] + step / 2, w + k1 / 2);
                k3 = step * f(xCoordinates[i] + step / 2, w + k2 / 2);
                k4 = step * f(xCoordinates[i] + step, w + k3);
                w = w + (k1 + 2 * k2 + 2 * k3 + k4) / 6;
                yCoordinates[i] = w;

            }
        }

        public void eulerMethod(Function f)
        {
            double temp = startY;
            yCoordinates = new double[xCoordinates.Length];

            yCoordinates[0] = startY;
            for (int i = 1; i < xCoordinates.Length; ++i)
            {
                temp = step * f(xCoordinates[i], temp);
                yCoordinates[i] = temp;
            }
        }


        public void implicitEulerMethod(Function f)
        {
            double temp = startY;
            double forwardEulerResult = startY;

            yCoordinates[0] = startY;
            for (int i = 1; i < xCoordinates.Length; ++i)
            {
                forwardEulerResult = temp;
                forwardEulerResult += step * f(xCoordinates[i], temp);
                temp += step * f(xCoordinates[i], forwardEulerResult);
                yCoordinates[i] = temp;

            }
        }

        public void explicitRungeKutta(Function f)
        {
            double k1, k2;
            double temp = startY;
            for (int i = 0; i < xCoordinates.Length; ++i)
            {
                k1 = step * f(xCoordinates[i], temp);
                k2 = step * f(xCoordinates[i] + (step / 2), temp + (k1 / 2));
                temp += k2;
                yCoordinates[i] = temp;
            }
        }




    }
}