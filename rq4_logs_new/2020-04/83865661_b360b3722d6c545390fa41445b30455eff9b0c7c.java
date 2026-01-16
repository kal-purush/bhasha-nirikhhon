package com.ctooley.plugins.economy;

import java.util.logging.Level;

import com.ctooley.plugins.SpawnerShop;

import org.bukkit.Bukkit;
import org.bukkit.entity.Player;

public class ShopEconomy {

    private SpawnerShop plugin;
    private ShopEconomyVault vault = null;
    private ShopEconomyReserve reserve = null;
    private API api;
    public boolean enabled = false;

    public ShopEconomy(SpawnerShop plugin)
    {
        this.plugin = plugin;
        enabled = setupEconomy();
    }

    public enum API {
        VAULT, RESERVE
    }

    public boolean setupEconomy()
    {
        vault = new ShopEconomyVault(plugin);
        if(vault.enabled)
        {
            api = API.VAULT;
            return true;
        }

        if(Bukkit.getPluginManager().getPlugin("Reserve") != null)
        {
            reserve = new ShopEconomyReserve();
            if(reserve.enabled)
            {
                api = API.RESERVE;
                return true;
            }
        }
        return false;
    }

    public void deposit(Player player, int amount)
    {
        switch(api) 
        {
            case VAULT:
                vault.deposit(player, amount);
                return;
            case RESERVE:
                reserve.deposit(player, amount);
                return;
        }
    }

    public void withdraw(Player player, int amount)
    {
        switch(api) 
        {
            case VAULT:
                vault.withdraw(player, amount);
                return;
            case RESERVE:
                reserve.withdraw(player, amount);
                return;
        }
    }

    public double getBalance(Player player)
    {
        switch(api) 
        {
            case VAULT:
                return vault.getBalance(player);
            case RESERVE:
                return reserve.getBalance(player);
        }
        return 0;
    }
}