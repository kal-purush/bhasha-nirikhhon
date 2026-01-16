package com.ctooley.plugins.util;

import com.ctooley.plugins.SpawnerShop;

import org.bukkit.plugin.RegisteredServiceProvider;

import net.milkbowl.vault.economy.Economy;

public class VaultAPI
{

    Economy economy;
    SpawnerShop plugin;

    public VaultAPI(SpawnerShop plugin)
    {
        this.plugin = plugin;
        setupEconomy();
    }

    private boolean setupEconomy() 
    {
        RegisteredServiceProvider<Economy> economyProvider = plugin.getServer().getServicesManager().getRegistration(Economy.class);

        if (economyProvider != null) 
        {
            economy = economyProvider.getProvider();
        }

        return economy != null;
    }
}