using System;

namespace KataFighter
{
    public class Program
    {
        public static void Main()
        {

        }

        public static string DeclareWinner(Fighter fighter1, Fighter fighter2, string firstAttacker)
        {
            // Your code goes here. Have fun!
            if (firstAttacker == fighter2.Name)
            {
                fighter1.Health = fighter1.Health - fighter2.DamagePerAttack;
                if (fighter1.Health <= 0)
                {
                    Console.WriteLine(fighter2.Name + "attacks " + fighter1.Name + "; " + fighter1.Name + " now has " + fighter1.Health + " health and is dead. " + fighter2.Name + " wins.");
                    return fighter2.Name;
                }
                else
                {
                    Console.WriteLine(fighter2.Name + "attacks " + fighter1.Name + "; " + fighter1.Name + " now has " + fighter1.Health + " health.");
                    return DeclareWinner(fighter1, fighter2, fighter1.Name);
                }
            }
            else
            {
                fighter2.Health = fighter2.Health - fighter1.DamagePerAttack;
                if (fighter2.Health <= 0)
                {
                    Console.WriteLine(fighter1.Name + "attacks " + fighter2.Name + "; " + fighter2.Name + " now has " + fighter2.Health + " health and is dead. " + fighter1.Name + " wins.");
                    return fighter1.Name;
                }
                else
                {
                    Console.WriteLine(fighter1.Name + "attacks " + fighter2.Name + "; " + fighter2.Name + " now has " + fighter2.Health + " health.");
                    return DeclareWinner(fighter1, fighter2, fighter2.Name);
                }
            }
        }
    }
}