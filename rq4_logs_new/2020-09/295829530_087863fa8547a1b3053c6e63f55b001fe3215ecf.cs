using System;

namespace Program
{
    class Item
    {
        private String name;
        private int attack;
        private int defense;
        private String type;

        public String Name { get; set; }
        public String Type { get; set; }
        public int Attack { get; set; }
        public int Defense { get; set; }

        public Item(String name, String type, int attack, int defense)
        {
            this.Name = name;
            this.Type = type;
            this.Attack = attack;
            this.Defense = defense;
        }
    }
}