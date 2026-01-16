package com.yeetdev.ezQueueHub;


import com.yeetdev.ezQueueHub.listeners.*;
import com.yeetdev.ezQueueHub.utils.*;
import org.bukkit.Bukkit;
import org.bukkit.entity.Player;
import org.bukkit.plugin.PluginManager;
import org.bukkit.plugin.java.JavaPlugin;

public class Main extends JavaPlugin {



    public void onEnable() {



        Bukkit.getScheduler().scheduleSyncRepeatingTask(this, new Runnable() {
            public void run() {
                for (Player p : Bukkit.getOnlinePlayers()) {
                    ScoreBoard.createHubScoreboard(p);
                }
            }
        }, 100L, 100L);
        PluginManager pm = Bukkit.getPluginManager();
        pm.registerEvents(new ScoreBoard(), this);
        pm.registerEvents(new Blocks(), this);
        pm.registerEvents(new DoubleJump(), this);
        pm.registerEvents(new EntityDamage(), this);
        pm.registerEvents(new Inventory(), this);
        pm.registerEvents(new PlayerJoin(), this);
        pm.registerEvents(new Weather(), this);

    }

}