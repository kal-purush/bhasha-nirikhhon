using System;
using System.Collections.Generic;
using RPGDatabase.Models.Character;
using RPGDatabase.Models.Enum;

namespace RPGDatabase.Models.GamePart
{
    public class LevelStory
    {
        public int Id { get; set; }

        public TypeEnum IleType { get; set; }

        public List<Monster> Monsters { get; set; }

        public int Level { get; set; }

        public int Pourcentage { get; set; }

        public LevelStory()
        {
            IleType = TypeEnum.WATER;
            Level = 1;
            Pourcentage = 0;
            Monsters = new List<Monster>();
        }
    }
}