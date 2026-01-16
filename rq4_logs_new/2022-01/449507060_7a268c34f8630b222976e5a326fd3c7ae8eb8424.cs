using Terraria;
using Terraria.ID;
using Terraria.ModLoader;
using Terraria.DataStructures;
using System;

namespace PaperMarioBadges.Buffs
{
	public class Dodge : ModBuff
    {

        public override void SetDefaults()
        {
            DisplayName.SetDefault("Lucky Day");
            Description.SetDefault("Currently invulnerable");
        }

        public override void Update(Player player, ref int buffIndex)
        {
            player.immune = true;
        }

    }
}