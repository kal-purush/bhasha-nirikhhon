using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

using TextAdventureCS.Items;

namespace TextAdventureCS.Actors
{
    class StoneEnemy : Actor
    {
        public StoneEnemy(string name, int maxHealth) : base(name, maxHealth)
        {

        }

        public override void TakeHit(int damage)
        {
            health -= damage;
        }

        public override Objects Loot()
        {
            return new PowerStone("powerstone", true);
        }
    }
}