package aybici.parkourplugin.parkours.hibernate;

import org.bukkit.Material;

import javax.persistence.Embeddable;

@Embeddable
public class BackBlockSet {
    private String backBlockString = new String();

    public void add(Material material){
        if(!this.contains(material)){
            backBlockString += getSingleMaterialString(material);
        }
    }

    public void remove(Material material){
        if(this.contains(material)){
            backBlockString.replaceAll(getSingleMaterialString(material), "");
        }
    }

    public boolean contains(Material material){
        return backBlockString.contains(getSingleMaterialString(material));
    }

    private String getSingleMaterialString(Material material){
        return "|"+material.name()+"|";
    }
}