using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace ConsoleApp18
{
    class Car
    {
        public string model{ get; set; }
        public string brand { get; set; }
        public int year { get; set; }
        public string color { get; set; }
        private int codan;
        protected int numberOfSteats;
        private string v;

        public Car()
        {
                
        }

        public Car(string model, string brand, int year, string color, int codan, int numberOfSteats)
        {
            this.model = model;
            this.brand = brand;
            this.year = year;
            this.color = color;
            this.codan = codan;
            this.numberOfSteats = numberOfSteats;
        }

        public Car(string v)
        {
            this.v = v;
        }

        public int GetCodan()
        {
            return codan;
        }
        public int GetNumberOfSteats()
        {
            return numberOfSteats;
        }
        protected void ChangeNumberOfSteats()
        {

        }
    }
}