using System;

namespace FacadePattern
{
    public interface ICarFacade
    {
        void Build();
    }
    
    public class Car : ICarFacade
    {
        private readonly CarBody _carBody = new CarBody();
        private readonly CarEngine _carEngine = new CarEngine();
        private readonly CarAccessories _carAccessories = new CarAccessories();
        
        public void Build()
        {
            Console.WriteLine("Building a car");
            
            _carBody.Build();
            _carEngine.Build();
            _carAccessories.Build();
        }
    }
}