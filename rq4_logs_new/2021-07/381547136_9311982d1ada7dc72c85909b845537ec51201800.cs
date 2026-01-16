using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace ConsoleGame
{
    public class Items
    {


        public string ItemName { get; }

        public int ItemDamage { get; }


        public Items(string itemName, int itemDamage)
        {
            ItemName = itemName;
            ItemDamage = itemDamage;
        }
    }
}   