package net.okuri.qol.qolCraft.superCraft;

import net.okuri.qol.PDCC;
import net.okuri.qol.PDCKey;
import net.okuri.qol.superItems.SuperItemType;
import org.bukkit.Material;
import org.bukkit.inventory.ItemStack;

import java.util.ArrayList;

public class ShapelessSuperCraftRecipe extends SuperCraftRecipe{
    //不定形レシピはsetMatrixに登録した順にSuperItem のItemStackがわたされるので注意!!

    private final ItemStack result;
    private SuperCraftable resultClass;
    private final ArrayList<Material> ingredients = new ArrayList<>();
    private final ArrayList<SuperItemType> superIngredients = new ArrayList<>();
    private ItemStack[] superIngredientItems;
    public ShapelessSuperCraftRecipe(ItemStack result) {
        this.result = result;
    }
    @Override
    public boolean checkSuperRecipe(ItemStack[] matrix) {
        superIngredientItems = new ItemStack[superIngredients.size()];
        for(ItemStack i : matrix){
            if(i != null){
                if(ingredients.contains(i.getType())){
                    continue;
                } else if (PDCC.has(i.getItemMeta(), PDCKey.TYPE)){
                    if(superIngredients.contains(SuperItemType.valueOf(PDCC.get(i.getItemMeta(), PDCKey.TYPE)))){
                        superIngredientItems[superIngredients.indexOf(SuperItemType.valueOf(PDCC.get(i.getItemMeta(), PDCKey.TYPE)))] = i;
                        continue;
                    } else{
                        return false;
                    }
                } else{
                    return false;
                }
            }
        }
        return true;
    }

    public void addIngredient(Material ingredient){
        this.ingredients.add(ingredient);
    }
    public void addIngredient(SuperItemType ingredient){
        this.superIngredients.add(ingredient);
    }
    @Override
    public void setResultClass(SuperCraftable resultClass) {
        this.resultClass = resultClass;
    }
    @Override
    public SuperCraftable getResultClass(){
        resultClass.setMatrix(superIngredientItems);
        return resultClass;
    }
}