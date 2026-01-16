using RPG.CharacterClasses;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace RPG.Items
{
    interface IConsumeable
    {
        string Name { get; set; }
        string Itemtype { get; set; }
        int Consumeeffect { get; set; }

        void Use(PlayerBase player);
        void Sell();
    
    }
}