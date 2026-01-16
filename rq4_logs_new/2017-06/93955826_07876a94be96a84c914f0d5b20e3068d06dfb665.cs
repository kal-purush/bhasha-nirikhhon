using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace SimpleClass
{
    public class Car
    {
        public int speed;
        public string name;


        // это стандартный, но переопределенный
        public Car()
        {
            speed = 40;
            name = "Просто";
        }
        //Этот специальный
        //А если сделать все параметры опциональными, то строить цепочку конструкторов не обязательно!!!!
        public Car(string MyCarName,int MyCarSpeed = 60)
        {
            name = MyCarName;
            speed = MyCarSpeed;
        }
        
        public void PrintInfo()
        {
            Console.WriteLine($"Name = {name}, speed = {speed}");
        }
        public void SpeedUp(int delta)
        {
            speed += delta;
        }
    }
}