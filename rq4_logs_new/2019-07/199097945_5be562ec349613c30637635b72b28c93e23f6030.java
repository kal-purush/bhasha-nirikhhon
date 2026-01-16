package me.captain.secret;

import java.io.File;
import java.util.HashMap;
import java.util.UUID;
import org.bukkit.Location;
import org.bukkit.Material;
import org.bukkit.block.Block;
import org.bukkit.configuration.file.YamlConfiguration;
import org.bukkit.entity.Player;
import org.bukkit.plugin.java.JavaPlugin;

/**
 * @author captainawesome7
 */
public class CaptainSecret extends JavaPlugin {
    
    // Storage for players creating or editing secrets
    public HashMap<Player, Waiting> waiting;
    // Storage of all active secret blocks
    public HashMap<Block, Secret> secrets;

    // Method runs on server enabling plugin after world load
    @Override
    public void onEnable() {
        waiting = new HashMap<>();
        secrets = new HashMap<>();
        getCommand("secret").setExecutor(new Cmd(this));
        getServer().getPluginManager().registerEvents(new SecretListener(this), this);
        loadSecrets();
    }

    // Method runs while server is stopping
    @Override
    public void onDisable() {
        saveSecrets();
    }

    // Loads secrets from a yml file and stores them for active use
    public void loadSecrets() {
        System.out.println("[CaptainSecret] Loading secrets.yml");
        try {
            File f = new File(getDataFolder(), "secrets.yml");
            YamlConfiguration secf = new YamlConfiguration();
            secf.load(f);
            for (String s : secf.getKeys(false)) {
                Integer time = secf.getInt(s + ".timer");
                UUID world = UUID.fromString(secf.getString(s + ".block.world"));
                Integer bx = secf.getInt(s + ".block.x");
                Integer by = secf.getInt(s + ".block.y");
                Integer bz = secf.getInt(s + ".block.z");
                Location block = new Location(getServer().getWorld(world), bx, by, bz);
                UUID uworld = UUID.fromString(secf.getString(s + ".under.world"));
                Integer ux = secf.getInt(s + ".under.x");
                Integer uy = secf.getInt(s + ".under.y");
                Integer uz = secf.getInt(s + ".under.z");
                Location under = new Location(getServer().getWorld(uworld), ux, uy, uz);
                Material mat = Material.valueOf(secf.getString(s + ".material"));
                Secret sec = new Secret(this, block, time, mat);
                sec.setUnder(under);
                this.secrets.put(block.getBlock(), sec);
            }
        } catch(Exception e) {
            System.out.println("[CaptainSecret] Error loading secrets.yml");
            e.printStackTrace();
        }
    }

    // Saves active secrets and stores them in a yml file
    public void saveSecrets() {
        System.out.println("[CaptainSecret] Saving secrets.yml");
        try {
            File f = new File(getDataFolder(), "secrets.yml");
            YamlConfiguration secf = new YamlConfiguration();
            Integer i = 0;
            for (Block b : secrets.keySet()) {
                Secret s = secrets.get(b);
                secf.set("secret" + i.toString() + ".block.world", b.getLocation().getWorld().getUID().toString());
                secf.set("secret" + i.toString() + ".block.x", b.getLocation().getBlockX());
                secf.set("secret" + i.toString() + ".block.y", b.getLocation().getBlockY());
                secf.set("secret" + i.toString() + ".block.z", b.getLocation().getBlockZ());
                secf.set("secret" + i.toString() + ".under.world", s.getUnder().getWorld().getUID().toString());
                secf.set("secret" + i.toString() + ".under.x", s.getUnder().getBlockX());
                secf.set("secret" + i.toString() + ".under.y", s.getUnder().getBlockY());
                secf.set("secret" + i.toString() + ".under.z", s.getUnder().getBlockZ());
                secf.set("secret" + i.toString() + ".timer", s.getTime());
                secf.set("secret" + i.toString() + ".material", s.mat.toString());
                i++;
            }
            secf.save(f);
        } catch (Exception e) {
            System.out.println("[CaptainSecret] Error saving secrets.yml");
            e.printStackTrace();
        }
    }
}