package com.ctooley.plugins.util;

import com.ctooley.plugins.SpawnerShop;

import org.bukkit.ChatColor;
import org.bukkit.block.Sign;

public class ShopSign 
{
    private int price;
    private String spawnerType;
    private String method;
    private boolean valid;

    public ShopSign(Sign sign)
    {
        valid = sign.getLine(0).equalsIgnoreCase(ChatColor.BLUE + "[SpawnerShop]");
        spawnerType = sign.getLine(2).toLowerCase();
        method = sign.getLine(1).toLowerCase();
        setPrice(sign.getLine(3));
    }

    public int getPrice()
    {
        return this.price;
    }

    public String getSpawnerType()
    {
        return this.spawnerType;
    }

    public String getMethod()
    {
        return this.method;
    }

    public boolean isValid()
    {
        return this.valid;
    }

    private void setPrice(String line)
    {
        String price = line.replace(SpawnerShop.currencySign, "").replace(",", "");
        if(isInt(price))
        {
            this.price = Integer.parseInt(price);
        }
        else
        {
            this.price = -1;
        }
    }

    private boolean isInt(String s) {
        try {
            Integer.parseInt(s);
            return true;
        } catch (NumberFormatException ex) {
            return false;
        }
    }
}