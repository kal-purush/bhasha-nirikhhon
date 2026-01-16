package net.okuri.qol.qolCraft.distillation;

import net.okuri.qol.superItems.SuperItemType;
import org.bukkit.inventory.ItemStack;

import java.util.ArrayList;

public class DistillationRecipe {
    // 注意：　ここでのingredients は、全てが必須なのではなく、どれか一つでもあれば良い。
    private final ArrayList<SuperItemType> ingredients = new ArrayList<>();
    private final Distillable resultClass;
    private final String recipeName;

    public DistillationRecipe(String name, Distillable resultClass) {
        this.resultClass = resultClass;
        this.recipeName = name;
    }
    public void addingredient(SuperItemType superIngredient){
        ingredients.add(superIngredient);
    }
    private String getRecipeName(){
        return recipeName;
    }
    public Distillable getResultClass(){
        return resultClass;
    }
    public ArrayList<SuperItemType> getIngredients(){
        return ingredients;
    }

}