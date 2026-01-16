package com.ctooley.plugins.util;

import java.io.File;
import java.io.IOException;

import com.ctooley.plugins.SpawnerShop;

import org.bukkit.configuration.InvalidConfigurationException;
import org.bukkit.configuration.file.FileConfiguration;
import org.bukkit.configuration.file.YamlConfiguration;

public class SpawnerFile 
{
    private SpawnerShop spawnerShop;
    private FileConfiguration spawnerConfig;
    private File spawnerFile;

    public SpawnerFile(SpawnerShop spawnerShop)
    {
        this.spawnerShop = spawnerShop;
        createConfig();
        spawnerConfig = getConfig();
    }

    public FileConfiguration getConfig()
    {
        return this.spawnerConfig;
    }

    private void createConfig() {
        spawnerFile = new File(spawnerShop.getDataFolder(), "spawners.yml");
        if (!spawnerFile.exists()) {
            spawnerFile.getParentFile().mkdirs();
            spawnerShop.saveResource("spawners.yml", false);
         }

        spawnerConfig = new YamlConfiguration();
        try {
            spawnerConfig.load(spawnerFile);
        } catch (IOException | InvalidConfigurationException e) {
            e.printStackTrace();
        }
    }
}