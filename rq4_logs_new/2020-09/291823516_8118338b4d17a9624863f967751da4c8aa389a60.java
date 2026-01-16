package ro.Stellrow.UpgradeableFurnaces;

import org.bukkit.Material;
import org.bukkit.block.Furnace;
import org.bukkit.event.EventHandler;
import org.bukkit.event.Listener;
import org.bukkit.event.block.BlockBreakEvent;
import org.bukkit.event.block.BlockPlaceEvent;
import org.bukkit.persistence.PersistentDataType;

public class FastFurnaceEvents implements Listener {
    private UltimateFurnace pl;

    public FastFurnaceEvents(UltimateFurnace pl) {
        this.pl = pl;
    }
    public void init(){
        pl.getServer().getPluginManager().registerEvents(this,pl);
    }



    @EventHandler
    public void onPlace(BlockPlaceEvent event){
        if(event.getItemInHand().getType()!= Material.FURNACE){
            return;
        }
        if(event.getItemInHand().hasItemMeta()){
            if(event.getItemInHand().getItemMeta().getPersistentDataContainer().has(pl.furnaceKey,PersistentDataType.STRING)){
                Furnace furnace = (Furnace) event.getBlockPlaced().getState();
                pl.getSqLite().addFurnace(furnace);
                pl.getUpgradeableFurnacesManager().addFurnace(furnace);
                furnace.getPersistentDataContainer().set(pl.furnaceKey,PersistentDataType.STRING,"UltimateFurnace");
                furnace.update();
            }
        }
    }

    @EventHandler
    public void onBreak(BlockBreakEvent event){
        if(event.getBlock().getType()==Material.FURNACE){
            Furnace furnace = (Furnace) event.getBlock().getState();
            if(furnace.getPersistentDataContainer().has(pl.furnaceKey,PersistentDataType.STRING)){
                pl.getSqLite().removeFurnace(furnace);
                pl.getUpgradeableFurnacesManager().removeFurnace(furnace);
                event.setDropItems(false);
                event.getBlock().getWorld().dropItemNaturally(event.getBlock().getLocation(), pl.furnace);
            }
        }
    }

}