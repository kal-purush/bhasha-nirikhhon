package me.reinatix.MoGolems;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.bukkit.Bukkit;
import org.bukkit.ChatColor;
import org.bukkit.configuration.file.YamlConfiguration;
import org.bukkit.plugin.PluginManager;
import org.bukkit.plugin.java.JavaPlugin;

import me.reinatix.MoGolems.commands.Spawner;
import me.reinatix.MoGolems.events.BlockBreak;
import me.reinatix.MoGolems.events.BlockPlace;
import me.reinatix.MoGolems.events.EntityDeath;
import me.reinatix.MoGolems.events.SpawnerSpawn;
import me.reinatix.MoGolems.utils.Utils;

public class Main extends JavaPlugin {

	public static Main instance;
	public static Utils utils;

	public List<String> ironLocations = new ArrayList<String>();
	public List<String> goldLocations = new ArrayList<String>();
	public List<String> diamondLocations = new ArrayList<String>();
	public List<String> emeraldLocations = new ArrayList<String>();

	@Override
	public void onEnable() {
		instance = this;
		utils = new Utils();
		Bukkit.getLogger().info(ChatColor.GREEN + "MoGolems has been enabled!");
		registerCommands();
		registerFiles();
		registerMessages();
		registerEvents();
		saveDefaultConfig();
	}

	public void registerFiles() {
		File f = new File(getDataFolder() + File.separator + "messages.yml");
		if (!f.exists()) {
			try {
				f.createNewFile();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}
	
	public void registerMessages() {
		setMessage("config-reload", "&a&lSUCCESS: &aConfig has been reloaded!");
		setMessage("not-enough-arguments", "&c&lERROR: &cYou didn''t use enough arguments! Try /spawner?");
		setMessage("player-null", "&c&lERROR: &cThat player is offline or you spelt their name wrong!");
		setMessage("received-spawner", "&e&lATTENTION: &eYou''ve just received a &6%type% &espawner!");
		setMessage("gave-player-spawner", "&a&lSUCCESS: &aYou''ve just gave &6%player% &ea &6%type% &espawner!");
		setMessage("no-permission", "&c&lERROR: &cYou don''t have the correct permissions for that!");
	}

	public void registerCommands() {
		getCommand("spawner").setExecutor(new Spawner());
	}

	public void registerEvents() {
		PluginManager pm = Bukkit.getServer().getPluginManager();
		pm.registerEvents(new BlockBreak(), this);
		pm.registerEvents(new BlockPlace(), this);
		pm.registerEvents(new EntityDeath(), this);
		pm.registerEvents(new SpawnerSpawn(), this);
	}

	public Utils getUtils() {
		return utils;
	}

	private void setMessage(String name, String message) {
		File f = new File(getDataFolder() + File.separator + "messages.yml");
		YamlConfiguration config = YamlConfiguration.loadConfiguration(f);
		if (!config.isSet(name)) {
			config.set(name, message);
			try {
				config.save(f);
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}

}