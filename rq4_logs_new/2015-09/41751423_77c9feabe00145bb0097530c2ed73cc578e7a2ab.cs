using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace mst_boredom_remover
{
    class Player
    {
        public int id;
        public string name;
        public int team;
        public double gold;
        public double mana_cystals;
        public double iron;
        public bool is_alive;
        public int unit_count;

        public enum PlayerModifier
        {
            Efficient,
            Lazy,
            Hated,
            Loved
        };
        public List<PlayerModifier> modifiers;

        
    }
}