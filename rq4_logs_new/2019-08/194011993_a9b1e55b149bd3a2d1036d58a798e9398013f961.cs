using Terraria;
using Terraria.DataStructures;
using Terraria.ID;
using Terraria.ModLoader;

namespace ShieldMod.Items
{
    public class Tritium : ModItem
    {
        public override void SetStaticDefaults()
        {
            Tooltip.SetDefault("Is it going to explode?]");
            Main.RegisterItemAnimation(item.type, new DrawAnimationVertical(8, 3));
        }
        public override void SetDefaults()
        {
            item.damage = 0;
            item.magic = true;
            item.width = 8;
            item.height = 8;
            item.maxStack = 999;
            item.consumable = true;
            item.knockBack = 1.5f;
            item.value = Item.sellPrice(0, 2, 50, 0);
            item.rare = 6;
            item.shoot = mod.ProjectileType("GeneratorEX");
            item.ammo = mod.ItemType("Deuterium");
        }
        public override void AddRecipes()
        {
            ModRecipe recipe = new ModRecipe(mod);
            recipe.AddIngredient(mod.GetItem("Deuterium"));
            recipe.AddIngredient(ItemID.Ectoplasm, 10);
            recipe.AddTile(TileID.CrystalBall);
            recipe.SetResult(this);
            recipe.AddRecipe();
        }
    }
}