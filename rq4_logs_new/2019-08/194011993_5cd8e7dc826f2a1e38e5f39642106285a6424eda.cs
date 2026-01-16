using Microsoft.Xna.Framework;
using System;
using Terraria;
using Terraria.ID;
using Terraria.ModLoader;
using Terraria.DataStructures;

namespace ShieldMod.Buffs
{
    public class Protect : ModBuff
    {
        public override void SetDefaults()
        {
            DisplayName.SetDefault("Protected");
            Description.SetDefault("The shield guards you");
            Main.buffNoTimeDisplay[Type] = true;
            Main.debuff[Type] = false; //Add this so the nurse doesn't remove the buff when healing
        }
        double angle = 0;
        double tangle = 0;
        public override void Update(Player player, ref int buffIndex)
        {
            player.immune = true;
            player.immuneAlpha = 0;
            angle += 91.5;
            tangle = angle * (Math.PI / 180);
            double x = (Math.Cos(tangle) * 30) + +player.Center.X;
            double y = (Math.Sin(tangle) * 30) + +player.Center.Y;
            Vector2 dustSp = new Vector2((float)x, (float)y);
            if (Math.Abs(dustSp.X) >= 0.12f)
            {
                Dust dust;
                dust = Dust.NewDustPerfect(dustSp, 226, new Vector2(0f, 0f), 30, new Color(255, 255, 255), 1.2f);
                dust.rotation = 50;
                dust.noGravity = true;
                dust.fadeIn = 1.15f;
            }
        }
    }
}