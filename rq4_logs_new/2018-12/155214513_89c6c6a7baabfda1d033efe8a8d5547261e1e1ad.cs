using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace LazyGeneral
{
    class Army
    {
        //=======================Конструктор=======================//
        public Army(double startPower, int startLocationX, int startLocationY, int startTeam, int startNumber)
        {
            team = startTeam;
            power = startPower;
            location[0] = startLocationX;
            location[1] = startLocationY;
            number = startNumber;
        }

        //=======================Необходимые параметры=======================//
        private double power;
        private int[] location = new int[2];
        private int number;
        private bool inFight;
        private int team;
        private const double basicPower = 50.0;

        //=======================Свойства=======================//
        public double Power
        {
            get { return power; }
            set
            {
                if (power > 0)
                    power = value;
                else power = 0;
            }
        }
        public int[] Location
        {
            get { return location; }
            set
            {
                if (location[0] > 0)
                    location[0] = value[0];
                if (location[1] > 0)
                    location[1] = value[1];
            }
        }
        public int Number
        {
            get { return number; }
        }
        public bool InFight
        {
            get { return inFight; }
            set { inFight = value; }
        }
        public double BasicPower
        {
            get { return basicPower; }
        }
        public int Team
        {
            get { return team; }
        }
    }
}