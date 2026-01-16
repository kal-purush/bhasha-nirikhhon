using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;


namespace RPG
{
    class Shop
    {
        private const string WEAPONS_FILENAME = "Resource/weapons.json";

        private Player player;

        private List<Weapon> weapons;

        /**
         * DEFAULT CONSTRUCTOR
         */
        public Shop(Player player)
        {
            this.player = player;
            this.weapons = new List<Weapon>();
        }

        /**
         * FUNCTION: openShop
         * IMPORT: None EXPORT: None
         * Handles the main flow of the shop
         */
        public void openShop()
        {
            bool shopOpen = true;

            // Generate all items required
            generateWeapons();

            while (shopOpen)
            {
                shopOpen = selectMenuOptions();
            }

            Console.WriteLine("Exiting Shop...\n");
        }

        /**
         * FUNCTION: selectMenuOptions
         * IMPORT: None
         * EXPORT: (bool) success
         * Allows player to select menu option
         */
        private bool selectMenuOptions()
        {
            bool success = true;
            int selection;

            // Show the menu to the user
            displayMainMenu();
            Int32.TryParse(Console.ReadLine(), out selection);

            switch (selection)
            {
                case 1:
                    // Display all weapons
                    displayWeapons();
                    buyWeapon();
                    break;
                case 2:
                    // Display all armour
                    break;
                case 3:
                    // Display all items
                    break;
                case 4:
                    // Exit
                    success = false;
                    break;
                default:
                    Console.WriteLine("Invalid option. Please try again\n");
                    break;
            }

            return success;
        }

        /**
         * FUNCTION: displayMainMenu
         * IMPORT: None EXPORT: None
         * Displays the shop main menu to the user
         */
        private void displayMainMenu()
        {
            Console.WriteLine("\n===== [ Shop ] =====");

            // All available current options
            Console.WriteLine("[1] Weapons");
            Console.WriteLine("[2] Armour");
            Console.WriteLine("[3] Items");
            Console.WriteLine("[4] Exit");

            Console.WriteLine("");
            Console.Write("Enter option: ");
        }

        /**
         * FUNCTION: generateWeapons
         * IMPORT: None EXPORT: None
         * Generates a list of weapons to be used within the shop
         */
        private void generateWeapons()
        {
            // No way of parsing any external data unless we want to use raw data
            // So just hard coding in the items

            // Weapons ( name, price, strengthBonus, defenceBonus, healthBonus, levelRequirement)
            weapons.Add(new Weapon("Iron Dagger", 10, 1, 0, 0, 1));
            weapons.Add(new Weapon("Iron Shortsword", 20, 2, 1, 5, 2));
            weapons.Add(new Weapon("Steel Dagger", 15, 2, 0, 10, 2));
            weapons.Add(new Weapon("Steel Shortsword", 30, 3, 2, 20, 1));
        }

        /**
         * FUNCTION: displayWeapons
         * IMPORT: None EXPORT: None
         * Displays all available weapons to the user
         */
        private void displayWeapons()
        {
            int ii = 0;
            Console.WriteLine("\n===== [ WEAPONS ] =====");

            // Loop through the lis of weapons
            foreach(Weapon weapon in weapons)
            {
                // Only show weapon if the player can use it
                if (player.getLevel() >= weapon.LevelRequirement)
                {
                    Console.WriteLine(weapon.Name);
                    Console.WriteLine("ID: " + ii);
                    Console.WriteLine("Price: " + weapon.Price);
                    Console.WriteLine("Strength: " + weapon.StrengthBonus);
                    Console.WriteLine("Defence: " + weapon.DefenceBonus);
                    Console.WriteLine("Health Bonus: " + weapon.HealthBonus);
                    Console.WriteLine("Level: " + weapon.LevelRequirement);
                    Console.WriteLine("");

                    
                }
                ii++;
            }

            Console.WriteLine("\nEnter Weapon ID: ");
        }

        /**
         * FUNCTION: buyWeapon
         * IMPORT: None EXPORT: None
         * Handles the buying of weapons
         */
        private void buyWeapon()
        {
            // Get weapon ID
            int id;
            Int32.TryParse(Console.ReadLine(), out id);

            // Convert to array
            Weapon[] weaponArr = weapons.ToArray();

            // Check ID is valid
            if (id > weapons.Count() || id < 0)
            {
                Console.WriteLine("Invalid weapon ID");
            }
            else
            {
                Weapon weapon = weaponArr[id];
                if (weapon.LevelRequirement > player.getLevel())
                {
                    Console.WriteLine("Invalid weapon ID");
                }
                else
                {
                    // Check player isn't broke ass
                    if (weapon.Price > player.getMoney())
                    {
                        Console.WriteLine("Not enough money to buy this weapon");
                    }
                    else
                    {
                        // TODO: add weapon to player when inventory is sorted
                        Console.WriteLine("Successfully purchased " + weapon.Name + " for $" + weapon.Price);
                    }
                }
            }
        }

    }
}