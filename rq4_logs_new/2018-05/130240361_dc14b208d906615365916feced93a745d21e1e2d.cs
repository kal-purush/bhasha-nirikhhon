using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Threading;

namespace Inventory
{
    class Program
    {
        static void Main(string[] args)
        {
            var player = new Player();
            Start();

            for (byte round = 0; round <= player.CapacityInventory(); round++)
            {
                Console.WriteLine("Choose items for you");
                Console.WriteLine($"BottleHP is 1");
                Console.WriteLine($"BottleMP is 2");
                Console.WriteLine($"bottleST is 3");

                byte.TryParse(Console.ReadLine(), out byte key);
                player.IntoInventory(player.inventory.SetItem(key));
                Console.Clear();
            }
            Console.WriteLine("You have filled your inventory!");
            Console.WriteLine("Start the game");

            foreach(var bottle in player.inventory)
            {
                Console.WriteLine(bottle.Name);
            }

            player.inventory.inventory.GetType();

            //Thread adding = new Thread();

            Console.ReadLine();
        }

        //static Item UseItem (byte key, Player player)
        //{
        //    player.ContentInventory
        //}



        static void Start()
        {
            Console.WriteLine("Welcome to my realy simple game!");
            Console.WriteLine("In this game you will try to stay alive.");
            Console.WriteLine("At first: Fill your Inventory!");
        }
    }
}