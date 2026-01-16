using System;
using System.Collections.Generic;

namespace Rainforest
{
    class Company
    {
        private int nextContainer = 0;
        public Dictionary<string, List<Warehouse>> ItemsSold;//string is item in warehouse / list of warehouses that contain item
        public Dictionary<string, List<Container>> ContainersNotInWarehouse;
        public Dictionary<string, List<Item>> UncontainedFruit;
        public Dictionary<string, List<Item>> FruitOfferings;

        private List<Warehouse> WarehousesOwned;
        //keep track of warehouses and build new warehouses

        public void Run(List<string> menu)
        {
            Display screen = new Display();
            string selection = "play";
            while (selection != "Exit")
            {
                selection = screen.Run(menu);

                if (selection == "Harvest Fruit")
                {

                }
                if (selection == "Pack Container")
                {

                }
                if (selection == "Build Warehouse")
                {

                }
                if (selection == "Send To Warehouse")
                {

                }
                if (selection == "Find Item")
                {

                }
            }
        }
        public void PickFruit()
        {
            List<string> fruits = new List<string>() { "Apple", "Pear", "Banana", "Strawberry", "New Fruit", "Exit" };
            Display screen = new Display();
            string selection = "pick";
            while (selection != "Exit")
            {
                selection = screen.Run(fruits);
                foreach (var item in fruits)
                {
                    if (selection == item)
                    {
                        break;
                    }
                }

            }

        }
        public void ConstructWarehouse(string city)
        {

        }

        public void ConstructContainer()
        {
            // build the container
            //add container to notInWarehouse containers
        }
        public void PackContainer(string containerID)
        {
            //put stuff in a container
        }
        public void ShipContainer(string containername)
        {
            //put container in dest warehouse
        }
    }
}