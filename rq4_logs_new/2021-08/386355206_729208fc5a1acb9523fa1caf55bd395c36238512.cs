using System;

namespace SpaceStation
{
    public class XWing : SpaceShip
    {
        public override void StartHyperEngine()
        {
            Console.WriteLine("Вжум вжум вжум");
        }

        public override void Flight()
        {
            Console.WriteLine("Вшшшшшууу");
        }

        public override void Fire()
        {
            Console.WriteLine("Пиу пиу!");
        }

        public override void Docking(SpaceShip spaceShip)
        {
            Console.WriteLine($"Стыковка {spaceShip} в ячейку {StationGarageEnum.FirstSpot}");
        }
    }

   public delegate void FireDelegate();
   
    
    class Program
    {
        static void Main(string[] args)
        {
            CrewMember captain = new CrewMember("John", "Smith", 347895, 34, "Captain");
            Console.WriteLine($"Name: {captain.FirstName} {captain.LastName}\nID: {captain.ID}\nAge: {captain.Age}" +
                              $"\nPosition: {captain.Position}");
            
            Console.WriteLine();
            
            XWing xWing = new XWing();
            xWing.StartHyperEngine();
            xWing.Flight();
            xWing.Fire();
            xWing.Docking(xWing);
            
            Console.WriteLine();
            
            FireDelegate fireDelegate = delegate { xWing.Fire(); };
            fireDelegate.Invoke();
            

        }
    }
}