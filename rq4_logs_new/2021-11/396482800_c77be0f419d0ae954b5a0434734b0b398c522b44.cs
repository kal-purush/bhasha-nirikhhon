using System;
using System.Collections.Generic;
using System.Text;

namespace HomeworkMargaret.OOP
{
    class Cheese
    {
        public string name;
        public string condition;
        public string typeOfMilk;
        public string color;
        public string taste;
        public string country;

        public static int counter;

        public Cheese(string name, string condition, string typeOfMilk, string color, string taste, string country)
        {
            this.name = name;
            this.condition = condition;
            this.typeOfMilk = typeOfMilk;
            this.color = color;
            this.taste = taste;
            this.country = country;

            counter++;
        }
    }

    class Homework1_Classes
    {
        public static void Start()
        {
            Cheese ch1 = new Cheese("Camamber", "soft", "cow milk", "white", "sweet", "France");
        }
    }
}