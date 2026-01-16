using System.Collections.Generic;
using System.Linq;

namespace legendary_crafter
{
    internal class DungeonGift
    {
        public string Abbr
        {
            get;
            set;
        }

        public string dungeon
        {
            get;
            set;
        }

        public string name
        {
            get;
            set;
        }

        public string token
        {
            get;
            set;
        }
    }

    internal static class DungeonStore
    {
        private static List<DungeonGift> dungeons = new List<DungeonGift>
        {
            new DungeonGift {Abbr = "AC", dungeon = "Catacombs of Ascalon", name = "Gift of Ascalon", token = "Ascalonian Tear"},
            new DungeonGift {Abbr = "Arah", dungeon = "Ruined City of Arah", name = "Gift of Zhaitan", token = "Shard of Zhaitan"},
            new DungeonGift {Abbr = "CM", dungeon = "Caudecus Manor", name = "Gift of the Nobleman", token = "Seal of Beetletun"},
            new DungeonGift {Abbr = "COE", dungeon = "Crucible of Eternity", name = "Gift of Knowledge", token = "Knowledge Crystal"},
            new DungeonGift {Abbr = "FZ", dungeon = "Citadel of Flames", name = "Gift of Baelfire", token = "Flame Legion Charr Carving"},
            new DungeonGift {Abbr = "HOTW", dungeon = "Honor of the Waves", name = "Gift of Sanctuary", token = "Symbol of Koda"},
            new DungeonGift {Abbr = "SE", dungeon = "Sorrows Embrace", name = "Gift of the Forgeman", token = "Manifesto of the Moletartiate"},
            new DungeonGift {Abbr = "TA", dungeon = "Twilight Arbor", name = "Gift of Thorns", token = "Deadly Bloom"}
        };

        /// <summary>
        /// Gets the dungeon from the data base.
        /// </summary>
        /// <param name="id">The identifier.</param>
        /// <returns></returns>
        public static DungeonGift GetByCode(string id)
        {
            return dungeons.Single(a => a.Abbr == id);
        }
    }
}