using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Terraria;
using Terraria.ModLoader;
using CalamityMod;
using CalamityMod.Items.Weapons.Ranged;
using CalamityMod.Items.Weapons.Melee;

namespace FargoCalamity.Calamity.Ammos.Bullets
{
    public class AccelerationRound : BaseAmmo
    {
        public override int AmmunitionID => ModLoader.GetMod("CalamityMod").Find<ModItem>("AcceselerationRound").Type;
    }

    public class BloodfireBullet : BaseAmmo
    {
        public override int AmmunitionID => ModLoader.GetMod("CalamityMod").Find<ModItem>("BloodfireBullet").Type;
    }

    public class BubonicRound : BaseAmmo
    {
        public override int AmmunitionID => ModLoader.GetMod("CalamityMod").Find<ModItem>("BubonicRound").Type;
    }

    public class EnhancedNanoRound : BaseAmmo
    {
        public override int AmmunitionID => ModLoader.GetMod("CalamityMod").Find<ModItem>("EnhancedNanoRound").Type;
    }

    public class FalshRound : BaseAmmo
    {
        public override int AmmunitionID => ModLoader.GetMod("CalamityMod").Find<ModItem>("FlashRound").Type;
    }

    public class GodSlayerSlug : BaseAmmo
    {
        public override int AmmunitionID => ModLoader.GetMod("CalamityMod").Find<ModItem>("GodSlayerSlug").Type;
    }

    public class HolyFireBullet : BaseAmmo
    {
        public override int AmmunitionID => ModLoader.GetMod("CalamityMod").Find<ModItem>("HolyFireBullet").Type;
    }

    public class HyperiusBullet : BaseAmmo
    {
        public override int AmmunitionID => ModLoader.GetMod("CalamityMod").Find<ModItem>("HyperiusBullet").Type;
    }

    public class IcyBullet : BaseAmmo
    {
        public override int AmmunitionID => ModLoader.GetMod("CalamityMod").Find<ModItem>("IcyBullet").Type;
    }

    public class MarksmanRound : BaseAmmo
    {
        public override int AmmunitionID => ModLoader.GetMod("CalamityMod").Find<ModItem>("MarksmanRound").Type;
    }

    public class MarksmanRound : BaseAmmo
    {
        public override int AmmunitionID => ModLoader.GetMod("CalamityMod").Find<ModItem>("MarksmanRound").Type;
    }

    public class MortarRound : BaseAmmo
    {
        public override int AmmunitionID => ModLoader.GetMod("CalamityMod").Find<ModItem>("MortarRound").Type;
    }

    public class RubberMortarRound : BaseAmmo
    {
        public override int AmmunitionID => ModLoader.GetMod("CalamityMod").Find<ModItem>("RubberMortarRound").Type;
    }

    public class SuperballBullet : BaseAmmo
    {
        public override int AmmunitionID => ModLoader.GetMod("CalamityMod").Find<ModItem>("TerraBullet").Type;
    }

    public class VeriumBullet : BaseAmmo
    {
        public override int AmmunitionID => ModLoader.GetMod("CalamityMod").Find<ModItem>("MarksmanRound").Type;
    }
}