/***************************************
 Date Created: 6/29/2014
 Purpose: To store Item related strings.
 ***************************************/

using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace PersonDataProg
{
    class Item
    {
        private string itemType;
        private float itemCost;

        /******************
         All Constructors.
         ******************/

        public Item() { }

        public Item(string itemType, float itemCost)
        {
            this.itemType = itemType;

            if (ItemCost > 0)
                this.itemCost = itemCost;
        }

        /********************
          All Setter Methods
         ********************/

        public string ItemType
        {
            set { itemType = value; }
            get { return itemType; }
        }

        public float ItemCost
        {
            get { return itemCost; }
            set
            {
                if (value > 0)
                    itemCost = value;
            }
        }

        /*******************
          ToString OverLoad
         ******************/
        public override string ToString()
        {
            return "ITEM: " + itemType + " PRICE: " + itemCost;
        }
    }
}