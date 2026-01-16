package Crates;

import lombok.Getter;
import lombok.Setter;
import org.bukkit.Material;
import org.bukkit.inventory.ItemStack;
import util.Util;

import java.util.ArrayList;
import java.util.List;

@Getter
@Setter
public class Crate {


    private String name;
    private List<String> lore;
    private String displayName;
    private String material;
    private List<String> rewards = new ArrayList<>();


    public Crate(String name, String displayName, String material, List lore) {
        this.name = name;
        this.displayName = displayName;
        this.lore = lore;
        this.material = material;
    }

    public ItemStack giveCrate(int amount) {
        return Util.createItem(Util.color(displayName), 0, Material.matchMaterial(material), Util.colorList(lore), amount);
    }


}