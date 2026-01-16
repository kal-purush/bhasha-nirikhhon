using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Terraria;
using Terraria.ModLoader;
using CalamityMod;
using CalamityMod.Items.Armor.Aerospec;
using CalamityMod.Items.Accessories;
using CalamityMod.Items.Weapons.Ranged;
using CalamityMod.Items.Weapons.Melee;
using CalamityMod.Items.Pets;
using CalamityMod.Buffs.Pets;
using CalamityMod.Projectiles.Pets;

namespace FargoCalamity.Calamity.Ammos.Arrows
{
    public class IcecleArrow : BaseAmmo
    {
        public override int AmmunitionID => ModLoader.GetMod("CalamityMod").Find<ModItem>("IcecleArrow").Type;
    }
    public class ArcticArrow : BaseAmmo
    {
        public override int AmmunitionID => ModLoader.GetMod("CalamityMod").Find<ModItem>("ArcticArrow").Type;
    }
    public class BloodfireArrow : BaseAmmo
    {
        public override int AmmunitionID => ModLoader.GetMod("CalamityMod").Find<ModItem>("BloodfireArrow").Type;
    }
    public class VanquisherArrow : BaseAmmo
    {
        public override int AmmunitionID => ModLoader.GetMod("CalamityMod").Find<ModItem>("VanquisherArrow").Type;
    }
    public class TerraArrow : BaseAmmo
    {
        public override int AmmunitionID => ModLoader.GetMod("CalamityMod").Find<ModItem>("TerraArrow").Type;
    }
    public class ElysianArrow : BaseAmmo
    {
        public override int AmmunitionID => ModLoader.GetMod("CalamityMod").Find<ModItem>("ElysianArrow").Type;
    }
    public class NapalmArrow : BaseAmmo
    {
        public override int AmmunitionID => ModLoader.GetMod("CalamityMod").Find<ModItem>("NapalmArrow").Type;
    }
}