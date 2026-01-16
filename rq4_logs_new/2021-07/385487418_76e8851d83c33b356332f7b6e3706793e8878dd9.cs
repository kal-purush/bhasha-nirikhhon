using System;

namespace Hierarchy
{

    public abstract class Animal
    {
        protected string _animalName;
        protected string _animalType;
        protected double _animalWeight;
        protected int _foodEaten;

        protected Animal(string Name, string Type, double Weight)
        {
            _animalName = Name;
            _animalType = Type;
            _animalWeight = Weight;
        }

        public abstract void MakeSound();

        public abstract void Eat(Food food, int foodEaten);

        public abstract void DisplayInfo();
      
    }

    public abstract class Mammal : Animal
    {
        protected string _livingRegion;

        protected Mammal(string Name, string Type, double Weight, string LivingRegion) : base(Name, Type, Weight)
        {
            _livingRegion = LivingRegion;
        }

        public override void DisplayInfo()
        {
            Console.WriteLine($"{_animalName} [{_animalType}, {_animalWeight}, {_livingRegion}, {_foodEaten}]");
        }
    }

    public abstract class Felime : Mammal
    {
        protected Felime(string Name, string Type, double Weight, string LivingRegion) : base(Name, Type, Weight, LivingRegion)
        {          
        }
    }

    public class Cat : Felime
    {
        private string _breed;

        public Cat(string Name, string Type, double Weight,string LivingRegion, string Breed) : base(Name, Type, Weight, LivingRegion)
        {
            _breed = Breed;
        }

        public override void DisplayInfo()
        {
            Console.WriteLine($"{_animalName} [{_animalType}, {_breed}, {_animalWeight}, {_livingRegion}, {_foodEaten}]");
        }

        public override void MakeSound()
        {
            Console.WriteLine("Meowwww");
            Console.Beep(1000, 1000);
        }

        public override void Eat(Food food, int foodEaten)
        {
            if (food.FoodType() == "Vegetable" || food.FoodType() == "Meat") 
            {
                this._foodEaten += foodEaten;
            }
            else 
            {
                Console.WriteLine(this._animalName + "s are not eating that type of food");
            }
        }
    }
    public class Tiger : Felime
    {
        private string _breed;

        public Tiger(string Name, string Type, double Weight, string LivingRegion) : base(Name, Type, Weight, LivingRegion)
        {
        }

        public override void MakeSound()
        {
            Console.WriteLine("ROAAR!!!");
            Console.Beep(200, 1000);
        }

        public override void Eat(Food food, int foodEaten)
        {
            if (food.FoodType() == "Meat") 
            {
                this._foodEaten += foodEaten;
            }
            else 
            {
                Console.WriteLine(this._animalName + "s are not eating that type of food");
            }
        }
    }

    public class Mouse : Mammal
    {
        public Mouse(string Name, string Type, double Weight, string LivingRegion) : base(Name, Type, Weight, LivingRegion)
        {
        }

        public override void MakeSound()
        {
            Console.WriteLine("piiiiiiiii...piii");
            Console.Beep(6000, 1000);
        }

        public override void Eat(Food food, int foodEaten)
        {
            if (food.FoodType() == "Vegetable") 
            {               
                this._foodEaten += foodEaten;
            }
            else 
            {
                Console.WriteLine(this._animalName + "s are not eating that type of food");
            }
        }
    }

    public class Zebra : Mammal
    {
        public Zebra(string Name, string Type, double Weight, string LivingRegion) : base(Name, Type, Weight, LivingRegion)
        {
        }

        public override void MakeSound()
        {
            Console.WriteLine("Prrr...Frrrr.....");
            Console.Beep(300, 1000);
        }

        public override void Eat(Food food, int foodEaten)
        {
            if (food.FoodType() == "Vegetable")
            {              
                this._foodEaten += foodEaten;
            }
            else 
            {
                Console.WriteLine(this._animalName + "s are not eating that type of food");
            }
        }
    }
}