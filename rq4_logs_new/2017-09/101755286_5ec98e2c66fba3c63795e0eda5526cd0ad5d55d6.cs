using System;

namespace HeadFirstStarbucksCoffee
{
    // ABSTRACT COMPONENT
    public abstract class Beverage
    {
        public string description = "Unkown Beverage";


        public abstract string getDescription();
       

        // public void setDescription(string value)
        // {
        //     description = value;
        // }

        public abstract double cost();

    }

}