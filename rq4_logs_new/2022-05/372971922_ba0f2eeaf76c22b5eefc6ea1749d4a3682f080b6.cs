using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace DesigPatterns.LazyInitialization
{
    class Car
    {
        public Car(string model)
        {
            Model = model;
        }

        public readonly string Model;
    }


    class CarDealer
    {
        private Dictionary<string, Car> availableCars = new Dictionary<string, Car>();

        public Car GetCar(string model)
        {
            if (!availableCars.TryGetValue(model, out Car car))
            {
                car = new Car(model);
                availableCars.Add(model, car);
            }
            return car;
        }
    }

    class Program
    {
        static void Main(string[] args)
        {
            var dealer = new CarDealer();
            var tesla1 = dealer.GetCar("Tesla model S");
            var mercedes = dealer.GetCar("Mercedes SE 200");
            Console.WriteLine(ReferenceEquals(tesla1, mercedes));

            var tesla2 = dealer.GetCar("Tesla model S");
            Console.WriteLine(ReferenceEquals(tesla1, tesla2));
        }
    }
}