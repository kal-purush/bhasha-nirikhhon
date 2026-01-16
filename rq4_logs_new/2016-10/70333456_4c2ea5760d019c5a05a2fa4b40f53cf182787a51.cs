using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace RPG
{
    class Item
    {
        private int price;
        private string name;

        // TODO: add more attributes for the item to modify from character

        /**
         * DEFAULT CONSTRUCTOR
         * IMPORT: None EXPORT: None
         */ 
        public Item()
        {
            this.price = 0;
            this.name = "DEFAULT ITEM";
        }

        /**
         * ALTERNATE CONSTRUCTOR 01
         * IMPORT: (int) price, (string) name
         * EXPORT: None
         */
        public Item(int price, string name)
        {
            this.price = price;
            this.name = name;
        }

        // Property for managing price
        public int Price
        {
            get { return price; }

            set {
                // Ensure value is within suited range
                if (value < 0)
                {
                    value = 0;
                }
                price = value;
            }
        }

        // Propertty for managing name
        public string Name
        {
            get { return name; }
            set { name = value; }
        }

    }
}