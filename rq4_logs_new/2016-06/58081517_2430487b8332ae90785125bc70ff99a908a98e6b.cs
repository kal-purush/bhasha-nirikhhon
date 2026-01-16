using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace ColdPlayProject.polymorphism.overloadding
{
    public class Eagle
    {
        private string _eagleName;
        private string _eagleColour;
        private int _eagleAge;

        public Eagle()
        {
            this._eagleName = "Mountian Eagle";
        }

        public Eagle(string eagleName)
        {
            this._eagleName = eagleName;
        }

   

        public Eagle(string eagleName, string eagleColour)
        {
            this._eagleName = eagleName;
            this._eagleColour = eagleColour;
        }

        public Eagle(int eagleAge)
        {
            this._eagleAge = eagleAge;
        }

        public void Fly(string eagleName, string wings)
        {
            Console.WriteLine("As an Eagle at 4years with 2 , I can fly at 5.00 speed/hour");
        }
        public void Fly(long eagleName)
        {
            Console.WriteLine("As an Eagle at 4years with 2 , I can fly at 5.00 speed/hour");
        }
        public void Fly()
        {
            Console.WriteLine("As an Eagle at 4years with 2 wings, I can fly at 5.00 speed/hour");
        }

        public void Fly(string birdName)
        {
            Console.WriteLine("As an {0} at 4years with 2 wings, I can fly at 5.00 speed/hour", birdName);
        }

        public void Fly(double speed)
        {
            Console.WriteLine("As an Eagle at 4years with 2 wings, I can fly at " + speed +" speed/hour");
        }

        public void Fly(int noOfWings)
        {
            Console.WriteLine("As an Eagle at 4years with {0} wings, I can fly at 5.00 speed/hour", noOfWings);
        }

        public void Fly(string birdName, int wings, double speed)
        {
            Console.WriteLine("As an {0} at 4years with {1} wings, I can fly at {2} speed/hour", birdName, wings, speed);
        }



        public string Land()
        {
            return "I have just petched on the ground";
        }

        public int Jump()
        {
            return 5;
        }
    }
}