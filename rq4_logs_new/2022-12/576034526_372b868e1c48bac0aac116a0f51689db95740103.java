package de.lbc.lobby;

import de.lbc.lobby.commands.*;
import de.lbc.lobby.listeners.*;
import de.lbc.lobby.utils.ActionBarMSG;
import de.lbc.lobby.utils.DayTimeRunnable;
import org.bukkit.Bukkit;
import org.bukkit.Location;
import org.bukkit.World;
import org.bukkit.configuration.file.FileConfiguration;
import org.bukkit.plugin.Plugin;
import org.bukkit.plugin.PluginManager;
import org.bukkit.plugin.java.JavaPlugin;
import org.bukkit.scheduler.BukkitTask;

public final class Main extends JavaPlugin {
    private static Plugin plugin;

    public static Plugin getPlugin() {
        return plugin;
    }

    private BukkitTask daytimetask;

    public static String navigatorName = "§rNavigator";

    public static String PREFIX = "[§cLobby§r] >> ";

    @Override
    public void onEnable() {
        // Plugin startup logic
        plugin = this;

        //Tasks
        ActionBarMSG.start();
        daytimetask = new DayTimeRunnable().runTaskTimer(this, 0, 20 * 5);

        //Commands
        getCommand("setspawn").setExecutor(new SetSpawnCommand());
        getCommand("spawn").setExecutor(new SpawnCommand());
        getCommand("navigator").setExecutor(new NavigatorEditorCommand());
        getCommand("maptp").setExecutor(new MapTPCommand());
        getCommand("villager").setExecutor(new VillagerCommand());
        getCommand("holo").setExecutor(new HoloCommand());
        getCommand("300622").setExecutor(new HiddenCommand());

        //Events
        PluginManager pm = Bukkit.getPluginManager();
        pm.registerEvents(new WeatherChangeListener(), this);
        pm.registerEvents(new JoinQuitListener(), this);
        pm.registerEvents(new BuildListener(), this);
        pm.registerEvents(new NavListener(), this);
        pm.registerEvents(new PlayerHeathListener(), this);

    }

    @Override
    public void onDisable() {
        //Disable Tasks
        ActionBarMSG.stop();
        daytimetask.cancel();
    }

    public static void setCfgLocation(String path, Location location) {
        FileConfiguration cfg = Main.getPlugin().getConfig();
        cfg.set(path + ".world", location.getWorld().getName());
        cfg.set(path + ".x", location.getX());
        cfg.set(path + ".y", location.getY());
        cfg.set(path + ".z", location.getZ());
        cfg.set(path + ".yaw", location.getYaw());
        cfg.set(path + ".pitch", location.getPitch());
        Main.getPlugin().saveConfig();
    }

    public static Location getCfgLocation(String path) {
        FileConfiguration cfg = Main.getPlugin().getConfig();
        String wldName = cfg.getString(path + ".world");
        if (wldName == null) return null;

        World world = Bukkit.getWorld(wldName);
        double x = cfg.getDouble(path + ".x");
        double y = cfg.getDouble(path + ".y");
        double z = cfg.getDouble(path + ".z");
        float yaw = (float) cfg.getDouble(path + ".yaw");
        float pitch = (float) cfg.getDouble(path + ".pitch");
        return new Location(world, x, y, z, yaw, pitch);
    }


}