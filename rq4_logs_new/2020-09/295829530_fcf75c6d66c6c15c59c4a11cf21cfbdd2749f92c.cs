using System;

namespace Program
{
    class TypeCharacter
    {
        private int attack;
        private int defense;
        private int magia;
        private int maxLife;
        public int Attack { get; set; }
        public int Defense { get; set; }
        public int Magia { get; set; }
        public int MaxLife { get; set; }

        private TypeCharacter(int attack, int defense, int magia, int maxLife)
        {
            this.Attack = attack;
            this.Defense = defense;
            this.Magia = magia;
            this.MaxLife = maxLife;
        }

        public static TypeCharacter StatSelector(String type)
        {
            TypeCharacter characterType = null;
            switch (type.ToLower())
            {
                case "mago":
                    {
                        characterType = new TypeCharacter(0, 5, 15, 100);
                        break;
                    }
                case "enano":
                    {
                        characterType = new TypeCharacter(10, 10, 0, 150);
                        break;
                    }
                case "elfo":
                    {
                        characterType = new TypeCharacter(5, 5, 5, 120);
                        break;
                    }
            }
            return characterType;
        }
    }
}