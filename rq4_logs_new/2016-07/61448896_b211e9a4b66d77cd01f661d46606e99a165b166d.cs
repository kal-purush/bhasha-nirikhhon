namespace BudgetTourist.Activities
{
    using System;
    using System.Collections.Generic;
    using System.Linq;
    using System.Text;
    using System.Threading.Tasks;

    using Contarcts;
    public abstract class ActivitiesAbstr : IActivities
    {
        private string name;
        private double price;
        private string location;
        private DateTime startTime;

        public ActivitiesAbstr(string name, double price, string location, DateTime startTime)
        {
            this.Name = name;
            this.Price = price;
            this.Location = location;
            this.StartTime = startTime;
        }

        public string Name { get; }
        public double Price { get; }
        public string Location { get; }
        public DateTime StartTime { get; }


        public double Pay(double money)
        {
            return money - price;
        }

        public override string ToString()
        {
            StringBuilder result = new StringBuilder();

            result.Append(Name + " activity starts at " + StartTime.ToString() + " and is located on " + Location
                + " and the price is " + Price + ".");

            return result.ToString();
        }

    }
}