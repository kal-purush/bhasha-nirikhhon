package fr.iglee42.techresourcecrystal.customize;

import fr.iglee42.techresourcecrystal.customize.crystaliserRecipe.CrystaliserRecipe;
import net.minecraft.world.item.crafting.RecipeSerializer;
import net.minecraft.world.item.crafting.RecipeType;
import net.minecraft.world.item.crafting.UpgradeRecipe;

import static net.minecraft.world.item.crafting.RecipeType.register;

public class CustomRecipes {

    public static RecipeType<CrystaliserRecipe> CRYSTALISER = RecipeType.register("crystaliser");
    public static RecipeSerializer<CrystaliserRecipe> CRYSTALISER_SERIA = RecipeSerializer.register("crystaliser", new CrystaliserRecipe.Serializer());



}