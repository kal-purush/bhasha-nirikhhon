using System.Collections;
using System.Collections.Generic;
using UnityEngine;

namespace Takens
{

    public enum ItemType { empty, keyOne, keyTwo}

    public class Inventory
    {

        private static Inventory _main = new Inventory();
        public static Inventory main
        {
            get
            {
                return _main;
            }
        }


        public bool hasFirstKey;
        public bool hasUnlockedDoorOne;
        public bool hasSecondKey;
        public bool hasUnlockedDoorTwo;
        public ItemType[] Inv;

        private Inventory() {}

    }
}