using System;

namespace Lab4.ConsoleApp.ClassType
{
    public class CarWithChainedConstructors
    {
        public string name;
        public int currSpeed;

        public CarWithChainedConstructors()
        {
        }

        public CarWithChainedConstructors(string name, int initialSpeed)
        {
            if(initialSpeed < 10)
            {
                initialSpeed = 10;
            }

            this.name = name;
            currSpeed = initialSpeed;
        }

        public CarWithChainedConstructors(int initialSpeed) : this("", initialSpeed)
        {

        }

        public CarWithChainedConstructors(string name) : this(name, 0)
        {

        }

        public void SpeedUp(int delta)
        {
            currSpeed += delta;
        }

        public void PrintState()
        {
            Console.WriteLine("Car name: {0}, going {1} KPH", name, currSpeed);
        }
    }
}