/*
 * This class is mostly used to collect all values
 * to save or load them into the app.
 */
package ffxiv.anima.weapon.tracker;

import java.io.File;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Iterator;

/**
 *
 * @author Workhorse
 */
public final class Collection implements Serializable {
    private int ubone_value;
    private int ushell_value;
    private int uore_value;
    private int useed_value;
    private int adamantite_francesca_value;
    private int titanium_alloy_mirror_value;
    private int dispelling_arrow_value;
    private int kingcake_value;
    private Collection loadedData;
    
    private ArrayList<Integer> savedValues;

    //attempts to load saved values
    public Collection() throws ClassNotFoundException{
        LoadData();
    }
    
    public Collection(int ubone, int ushell, int uore, int useed, int afran, int tallo, int darro, int kingcake){
        ubone_value = ubone;
        ushell_value = ushell;
        uore_value = uore;
        useed_value = useed;
        adamantite_francesca_value = afran;
        titanium_alloy_mirror_value = tallo;
        dispelling_arrow_value = darro;
        kingcake_value = kingcake;
    }
    

    
    public void SaveData(){
        File filename = new File("data.save");
        try{
            ObjectOutputStream out = new ObjectOutputStream(Files.newOutputStream(filename.toPath()));
            out.writeObject(this);
        } catch (IOException e){}
    }
    
    public void LoadData() throws ClassNotFoundException{
        File filename = new File("data.save");
        if (filename.exists()){
            try{
                ObjectInputStream in = new ObjectInputStream(Files.newInputStream(filename.toPath()));
                loadedData = (Collection) in.readObject();
            } catch (IOException e){}
            this.ubone_value = loadedData.ubone_value;
            this.ushell_value = loadedData.ushell_value;
            this.uore_value = loadedData.uore_value;
            this.useed_value = loadedData.useed_value;
            this.adamantite_francesca_value = loadedData.adamantite_francesca_value;
            this.titanium_alloy_mirror_value = loadedData.titanium_alloy_mirror_value;
            this.dispelling_arrow_value = loadedData.dispelling_arrow_value;
            this.kingcake_value = loadedData.kingcake_value;
        } else {
            //maybe set all values to 0, if false?
        }
    }
    
    public Iterator<Integer> returnValues(){
        savedValues = new ArrayList<>();
        savedValues.add(ubone_value);
        savedValues.add(ushell_value);
        savedValues.add(uore_value);
        savedValues.add(useed_value);
        savedValues.add(adamantite_francesca_value);
        savedValues.add(titanium_alloy_mirror_value);
        savedValues.add(dispelling_arrow_value);
        savedValues.add(kingcake_value);
        return savedValues.iterator();
    }
}