using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Objects2Multiple
{
    class City
    {
        private string name;
        private int houses = 0;
        private int wood = 0;
        public int water { get; set; }
        private int day = 0;
        public int food { get; set; }
        public int castle = 0;

        private List<Person> villagers = new List<Person>();
        private List<WaterSource> waterSources = new List<WaterSource>();
        private List<FoodSource> foodSources = new List<FoodSource>();

        public City(int pop)
        {
            Console.WriteLine("Name your city:");
            name = Console.ReadLine();
            water = 10;
            food = 10;
            houses = pop;
            castle = 0;
            for (int i = 0; i < pop; i++)
            {
                villagers.Add(new Person(this));
            }
            Console.WriteLine("Welcome to " + name + " with " + villagers.Count + " Population");
            Console.WriteLine("Goal is to keep your population alive and get to 10 population.");

            turn();
        }
        public int GetPop()
        {
            return villagers.Count;
        }

        public void IncreasePop(int amount)
        {
            for (int i = 0; i < amount; i++)
            {
                villagers.Add(new Person(this));
            }
        }

        public void IncreaseWood()
        {
            wood++;
        }
        public void IncreaseWood(int amount)
        {
            wood += amount;
        }

        public void BuildHouse()
        {
            if (wood >= 5)
            {
                wood -= 5;
                houses++;

                Console.WriteLine("You Built a house!");
            }
            else
            {
                Console.WriteLine("You didn't build a house! You need at least 5 wood.");
            }
        }

        public void BuildCastle()
        {
            if(wood >= 20)
            {
                wood -= 20;
                castle++;
                Console.WriteLine("Welcome to your castle!");
            }
            else
            {
                Console.WriteLine("You can't build a castle! You need at least 20 wood.");
            }
        }

        public void Win()
        {
            if (villagers.Count == 10)
            {
                Console.WriteLine("You win!");
            }
           
            if (castle ==1)
            {
                Console.WriteLine("You win!");
            }
        }

        public void BuildWell()
        {
            if (wood >= 6)
            {
                wood -= 6;
                waterSources.Add(new WaterSource("Well", 1));
                Console.WriteLine("You Built a well!");
            }
            else
            {
                Console.WriteLine("You didn't build a well! You need at least 6 wood.");
            }
        }

        public void killPerson(Person p)
        {
            villagers.Remove(p);
        }
        public void InfectWater()
        {
            water = 0;
        }
        public void InfectFood()
        {
            food = 0;
        }

        public void ChanceOfDeath()
        {
            if (GetPop() > 0)
            { string[] deathSituation = new string[] { "death", "water", "food" };
                Random situation = new Random();
                int i = situation.Next(0, 3);
                string choice = deathSituation[i];

                switch (choice)
                {
                    case "death":
                        Console.WriteLine("Smallpox has infected your village {0}  has died");
                        killPerson(villagers.ElementAt(0));
                        break;
                    case "water":
                        Console.WriteLine("Lead has poisoned your water. ");
                        InfectWater();
                        break;
                    case "food":
                        Console.WriteLine("Ebola has poisoned your food!");
                        InfectFood();
                        break;
                    default:
                        break;
                }
               
            }
            

        }

        public void Zombies()
        {
            if (GetPop() > 3)
            {
                killPerson(villagers.ElementAt(0));
                killPerson(villagers.ElementAt(1));
                Console.WriteLine("Zombies have attacked your village. Brains everywhere. {0} and {1} have died!");

            }
        }


        public int calculateWaterPerTurn()
        {
            int total = 0;
            foreach(WaterSource w in waterSources)
            {
                Console.WriteLine("Your " + w.name +" produces "+w.supply +" gallons of water per turn");
                total += w.supply;
            }
            Console.WriteLine("Your total water per turn is "+total);
            return total;
        }

        public int calculateFoodPerTurn()
        {
            int total = 0;
            foreach (FoodSource f in foodSources)
            {
                Console.WriteLine("Your " + f.name + " produces " + f.supply + " units of food per turn");
                total += f.supply;
            }
            Console.WriteLine("Your total food per turn is " + total);
            return total;
        }

        public void printWater()
        {
            Console.WriteLine(name + " has " + water+ " gallons of water");
        }
        public void printFood()
        {
            Console.WriteLine(name + " has " + food+  " food items" );
        }

        public void printPop()
        {
            Console.WriteLine("Your city has " + villagers.Count + " Population");
        }

        public void printWood()
        {
            Console.WriteLine("You have "+wood+" wood");
        }

        public void printHouses()
        {
            Console.WriteLine("Your city has "+houses+" houses");
        }

        public void printCastle()
        {
            Console.WriteLine("Your city has " +castle + " castles");
        }

        public void printStats()
        {
            printPop();
            printWater();
            printWood();
            printHouses();
            printFood();
            printCastle();
            Pause();
        }

        public void Pause()
        {
            Console.WriteLine("Press Enter to continue");
            Console.ReadLine();
        }

        public void turn()
        {
            day++;
            Console.WriteLine("__________________________________");
            Console.WriteLine("It's the start of day "+ day+"!");
            water += calculateWaterPerTurn();
            printStats();

            if (GetPop()>0)
            {
                if (houses > GetPop())
                {
                    int diff = houses - GetPop();
                    for(int i = 0; i < diff; i++)
                    {
                        Console.WriteLine("A new person has moved into your village!");
                        villagers.Add(new Person(this));
                    }
                }
                List<Person> deadVillagers = new List<Person>();
                for(int i = 0; i < villagers.Count; i++)
                {
                    Person p = villagers[i];
                    p.Work();
                }
                   
                //Eating loop
                foreach(Person p in villagers)
                {
                    if (p.Drink() == false)
                    {
                        //If they don't drink they die 
                        //We build up a seperate list since you can't modify 
                        //a list in a foreach loop
                        deadVillagers.Add(p);
                    }
                }
                printWater();
                Pause();
                foreach (Person p in villagers)
                {
                    if (p.Eat() == false)
                    {
                        deadVillagers.Add(p);
                    }
                }
                printFood();
                Pause();

                foreach (Person p in deadVillagers)
                {
                    //Remove dead people from the list 
                    //The list.remove() method searches by item and removes any matches
                    villagers.Remove(p);
                }
                turn();

                // Check for Chance of Death
                Random rnd = new Random();
                int ChanceofDeathAppears = rnd.Next(0, 21);
                if (ChanceofDeathAppears == 0)
                { ChanceOfDeath(); }

                turn();

                Random zombieattack = new Random();
                int ZombiesAttack = zombieattack.Next(0, 15);
                if (ZombiesAttack == 0)
                { Zombies(); }

                turn();

            }

            else
            {
                Console.WriteLine();
                Console.WriteLine("Everyone in "+name+" is dead, sorry!");
                Console.WriteLine("You made it to day "+day);
            }

        }
    }
}

