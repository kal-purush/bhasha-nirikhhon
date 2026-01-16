namespace MyCar
{
    public class Car
    {
        public List<Tire> Tires { get; set; }
        public List<Lamp> FrontLight { get; set; }
        public List<Lamp> RearLight { get; set; }
        public Engine engine { get; set; }

        public Car(List<Tire> carTires, List<Lamp> frontLights, List<Lamp> rearLights, Engine carEngine)
        {
            Tires = carTires;
            FrontLight = frontLights;
            RearLight = rearLights;
            engine = carEngine;
        }

        public void Drive()
        {
            Console.WriteLine("DRIVE");
        }
    }
}