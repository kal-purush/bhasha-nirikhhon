using System;

namespace FactoryDesignPattern
{
    class Program
    {
        static void Main(string[] args)
        {
            // Not illegal
            //var newObject = new FactoryStatic(); // <-- because of an abstract class
            
            // and you can't work around it
           // var newObject2 = new ConcreteFactory();

           var burger = BurgerFactory.CreateBurger("chicken");

           Console.WriteLine(burger.GetIngredients());
           
           var burger2 = new ChickenBurgerFactory().CreateBurger();

           Console.WriteLine(burger2.GetIngredients());
           
        }
    }
}