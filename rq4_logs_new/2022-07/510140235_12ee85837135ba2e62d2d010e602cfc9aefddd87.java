package me.zort.orbitaleco;

import me.zort.orbitaleco.commandExecutor.BalExecutor;
import me.zort.orbitaleco.commandExecutor.EarnExecutor;
import me.zort.orbitaleco.commandExecutor.GiveExecutor;
import me.zort.orbitaleco.commandExecutor.SetBalExecutor;
import org.bukkit.ChatColor;
import org.bukkit.configuration.file.FileConfiguration;
import org.bukkit.configuration.file.YamlConfiguration;
import org.bukkit.plugin.java.JavaPlugin;
import org.jetbrains.annotations.Nullable;

import java.io.File;
import java.io.IOException;

public final class OrbitalEco extends JavaPlugin {

    private static OrbitalEco INSTANCE;

    @Nullable
    public static OrbitalEco get() {
        return INSTANCE;
    }

    private FileConfiguration database;
    private FileConfiguration messages;

    @Override
    public void onEnable() {
        INSTANCE = this;
        File dataFile = getDatabaseFile();
        if(!dataFile.exists()) {
            saveResource("data.yml", false);
        }
        File messagesFile = new File(getDataFolder(), "messages.yml");
        if(!messagesFile.exists()) {
            saveResource("messages.yml", false);
        }
        database = YamlConfiguration.loadConfiguration(dataFile);
        messages = YamlConfiguration.loadConfiguration(messagesFile);
        getCommand("bal").setExecutor(new BalExecutor());
        getCommand("give").setExecutor(new GiveExecutor());
        getCommand("setbal").setExecutor(new SetBalExecutor());
        getCommand("earn").setExecutor(new EarnExecutor());
    }

    @Override
    public void onDisable() {
        saveDatabase();
    }

    public boolean saveDatabase() {
        try {
            database.save(getDatabaseFile());
            return true;
        } catch (IOException e) {
            return false;
        }
    }

    public void modifyBalance(String player, double amount) {
        setBalance(player, getBalance(player) + amount);
    }

    public void setBalance(String player, double balance) {
        database.set(player, balance);
    }

    public double getBalance(String player) {
        return database.getDouble(player, 0.0);
    }

    public String getMessage(String key, Object... args) {
        return ChatColor.translateAlternateColorCodes('&',
                String.format(messages.getString(key, ""), args));
    }

    private File getDatabaseFile() {
        return new File(getDataFolder(), "data.yml");
    }

}