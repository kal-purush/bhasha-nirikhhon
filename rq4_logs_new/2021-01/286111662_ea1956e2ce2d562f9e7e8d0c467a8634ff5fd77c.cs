using MyFirstBasicMod.Tiles;
using System.Collections.Generic;
using Microsoft.Xna.Framework;
using Terraria;
using Terraria.ID;
using Terraria.ModLoader;
using static Terraria.ModLoader.ModContent;

namespace MyFirstBasicMod.Items
{
	public class Banhammer : ModItem
	{
		public override void SetStaticDefaults() {
			DisplayName.SetDefault("Banhammer");
			Tooltip.SetDefault("One of the best hammers I've ever seen");
		}

		public override void SetDefaults() {
			item.damage = 85;
			item.melee = true;
			item.width = 40;
			item.height = 40;
			item.useTime = 4;
			item.useAnimation = 4;
			item.hammer = 105;      //How much hammer power the weapon has
			item.useStyle = ItemUseStyleID.SwingThrow;
			item.knockBack = 6;
			item.value = 99999;
			item.rare = ItemRarityID.Green;
			item.UseSound = SoundID.Item1;
			item.autoReuse = true;
		}

		public override void AddRecipes() {
			ModRecipe recipe = new ModRecipe(mod);
			recipe.AddIngredient(ModContent.ItemType<Items.Placeable.PinksBar>(), 19);
			recipe.AddIngredient(ModContent.ItemType<Items.EpicSoul>(), 5);
			recipe.SetResult(this);
			recipe.AddRecipe();
		}
	}
}
