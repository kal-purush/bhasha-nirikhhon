package com.cubepalace.cubechat;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.bukkit.ChatColor;
import org.bukkit.configuration.file.FileConfiguration;
import org.bukkit.plugin.java.JavaPlugin;

import com.comphenix.protocol.ProtocolLibrary;
import com.comphenix.protocol.ProtocolManager;
import com.cubepalace.cubechat.commands.ClearChat;
import com.cubepalace.cubechat.commands.CubeChatCmd;
import com.cubepalace.cubechat.commands.MuteChat;
import com.cubepalace.cubechat.commands.Shadowmute;
import com.cubepalace.cubechat.commands.ToggleFilter;
import com.cubepalace.cubechat.listeners.AdvertisingListener;
import com.cubepalace.cubechat.listeners.MiscChatListener;
import com.cubepalace.cubechat.listeners.MuteChatListener;
import com.cubepalace.cubechat.listeners.ShadowmuteListener;
import com.cubepalace.cubechat.util.ConfigFile;
import com.cubepalace.cubechat.util.Filter;
import com.cubepalace.cubechat.util.PlayerFile;

public class CubeChat extends JavaPlugin {

	private CubeChat instance;
	private boolean chatMuted;
	private List<UUID> noViewMutedChat;
	private List<UUID> noViewShadowmuted;
	private List<UUID> shadowmuted;
	private List<UUID> noFilter;
	private PlayerFile playerFile;
	private ConfigFile config;
	private Filter filter;
	private Map<UUID, Long> cooldownMap;
	private ProtocolManager pm;
	private final String noperm = ChatColor.RED + "No permission.";
	private boolean listsChanged = false;

	public CubeChat() {
		instance = this;
		chatMuted = false;
	}

	@Override
	public void onEnable() {
		getLogger().info("Loading player options from file...");
		setup();
		getLogger().info("Loading complete");
		getLogger().info("Scheduling automatic file saving...");
		saveTimer();
		getLogger().info("Automatic file saving enabled");
		getLogger().info("Enabling chat filter...");
		filter();
		getLogger().info("Filter enabled");
		register();
		getLogger().info("CubeChat has been enabled");
	}

	@Override
	public void onDisable() {
		if (listsChanged) {
			getLogger().info("Saving player options to file...");
			playerFile.updateMuteChatList();
			playerFile.updateIgnoreShadowmuted();
			playerFile.updateShadowmuted();
			playerFile.updateNoFilter();
			getLogger().info("Save complete");
			listsChanged = false;
		}
		getLogger().info("CubeChat has been disabled");
	}

	private void register() {
		getServer().getPluginManager().registerEvents(new MuteChatListener(this), this);
		getServer().getPluginManager().registerEvents(new ShadowmuteListener(this), this);
		getServer().getPluginManager().registerEvents(new MiscChatListener(this), this);
		getServer().getPluginManager().registerEvents(new AdvertisingListener(this), this);
		getCommand("clearchat").setExecutor(new ClearChat(this));
		getCommand("mutechat").setExecutor(new MuteChat(this));
		getCommand("cubechat").setExecutor(new CubeChatCmd(this));
		getCommand("shadowmute").setExecutor(new Shadowmute(this));
		getCommand("togglefilter").setExecutor(new ToggleFilter(this));
	}

	private void setup() {
		cooldownMap = new HashMap<UUID, Long>();
		if (!getDataFolder().exists())
			getDataFolder().mkdirs();
		playerFile = new PlayerFile(this, "players.yml");
		saveResource("config.yml", false);
		config = new ConfigFile(this, "config.yml");
		noViewMutedChat = playerFile.loadToListMuteChat();
		noViewShadowmuted = playerFile.loadToListIgnoreShadowmuted();
		shadowmuted = playerFile.loadToListShadowmuted();
		noFilter = playerFile.loadToListNoFilter();
	}

	private void saveTimer() {
		this.getServer().getScheduler().scheduleSyncRepeatingTask(this, new Runnable() {
			public void run() {
				if (listsChanged) {
					getLogger().info("Saving player options to file...");
					playerFile.updateMuteChatList();
					playerFile.updateIgnoreShadowmuted();
					playerFile.updateShadowmuted();
					playerFile.updateNoFilter();
					getLogger().info("Save complete");
					listsChanged = false;
				}
			}
		}, 6000L, 6000L);
	}

	private void filter() {
		pm = ProtocolLibrary.getProtocolManager();
		filter = new Filter(this);
		filter.filter();
	}

	public Filter getFilter() {
		return filter;
	}
	
	public String getNoPerm() {
		return noperm;
	}

	public CubeChat getInstance() {
		return instance;
	}

	public void toggleMute() {
		chatMuted = !chatMuted;
	}

	public boolean getChatMuted() {
		return chatMuted;
	}

	public List<UUID> getMutedChatIgnore() {
		return noViewMutedChat;
	}

	public void addIgnoreMutedChat(UUID uuid) {
		noViewMutedChat.add(uuid);
		listsChanged = true;
	}

	public void removeIgnoreMutedChat(UUID uuid) {
		noViewMutedChat.remove(uuid);
		listsChanged = true;
	}

	public PlayerFile getPlayerFile() {
		return playerFile;
	}

	public List<UUID> getShadowmuted() {
		return shadowmuted;
	}

	public void addShadowmuted(UUID uuid) {
		shadowmuted.add(uuid);
		listsChanged = true;
	}

	public void removeShadowmuted(UUID uuid) {
		shadowmuted.remove(uuid);
		listsChanged = true;
	}

	public List<UUID> getIgnoreShadowmuted() {
		return noViewShadowmuted;
	}

	public void addIgnoreShadowmuted(UUID uuid) {
		noViewShadowmuted.add(uuid);
		listsChanged = true;
	}

	public void removeIgnoreShadowmuted(UUID uuid) {
		noViewShadowmuted.remove(uuid);
		listsChanged = true;
	}

	public int getMaxCaps() {
		return getCustomConfig().getInt("maxCapitals");
	}

	public int getMaxFlood() {
		return getCustomConfig().getInt("maxFlood");
	}

	public long getSpamCooldown() {
		return getCustomConfig().getLong("spamCooldown");
	}

	public int getCensorDistance() {
		return getCustomConfig().getInt("censorDistance");
	}

	public Map<UUID, Long> getCooldowns() {
		return cooldownMap;
	}

	public void addCooldown(UUID uuid) {
		cooldownMap.put(uuid, System.currentTimeMillis());
	}

	public List<UUID> getNoFilter() {
		return noFilter;
	}

	public void addNoFilter(UUID uuid) {
		noFilter.add(uuid);
		listsChanged = true;
	}

	public void removeNoFilter(UUID uuid) {
		noFilter.remove(uuid);
		listsChanged = true;
	}

	public ProtocolManager getPM() {
		return pm;
	}
	
	public FileConfiguration getCustomConfig() {
		return config.getConfig();
	}
	
	public ConfigFile getConfigFile() {
		return config;
	}
}