using Terraria;
using Terraria.ModLoader;
using Terraria.ID;
using Terraria.Audio;
using MyFirstBasicMod.Items;

namespace MyFirstBasicMod.Items.Abomination
{
    public class AbominationBag : ModItem
    {
        public override void SetStaticDefaults()
        {
            DisplayName.SetDefault("Treasure Bag");
            Tooltip.SetDefault("{$CommonItemTooltip.RightClickToOpen}");
        }

        public override void SetDefaults()
        {
            Item.maxStack = 999;
            Item.consumable = true;
            Item.width = 24;
            Item.height = 24;
            Item.rare = ItemRarityID.Cyan;
            Item.expert = true;
        }

        public override bool CanRightClick()
        {
            return true;
        }

        public override void OpenBossBag(Player player)
        {
            player.TryGettingDevArmor();
            if (Main.rand.NextBool(7))
            {
                player.QuickSpawnItem(ModContent.ItemType<Items.Test>());
            }
            player.QuickSpawnItem(ModContent.ItemType<Items.EpicSoul>());
            player.QuickSpawnItem(ModContent.ItemType<Items.PinksWings>());
            player.QuickSpawnItem(ModContent.ItemType<Items.BreadPickaxe>());
        }

        public override int BossBagNPC => ModContent.NPCType<NPCs.Abomination.Abomination>();
    }
}