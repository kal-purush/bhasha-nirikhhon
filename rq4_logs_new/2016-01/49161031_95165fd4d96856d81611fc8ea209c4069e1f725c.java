package com.rebirthofbalkan.RoBHelp;

import org.bukkit.Bukkit;
import org.bukkit.command.Command;
import org.bukkit.command.CommandSender;
import org.bukkit.plugin.java.JavaPlugin;

public class RoBHelp
  extends JavaPlugin
{
  public void onEnable()
  {
    getConfig().options().copyDefaults(true);
    saveConfig();
    reloadConfig();
    if (getConfig().getInt("Config_Version") != 1.0D)
    {
      Bukkit.getLogger().warning("#========================================================#");
      Bukkit.getLogger().warning("#             ROBHELP - WARNING - UPDATE                 #");
      Bukkit.getLogger().warning("#  Your config is out of date! Please backup and delete  #");
      Bukkit.getLogger().warning("#        the current config to allow changes.            #");
      Bukkit.getLogger().warning("#========================================================#");
    }
  }
  
  public void onDisable() {}
  
  public boolean onCommand(CommandSender sender, Command cmd, String label, String[] args)
  {
    reloadConfig();
    if (cmd.getName().equalsIgnoreCase("help"))
    {
      String str = "";
      if (args.length < 1) {
        str = "help";
      }
      if (args.length >= 1) {
        str = args[0];
      }
      if (getConfig().isSet(str.toLowerCase()))
      {
        if (sender.hasPermission("help." + str.toLowerCase()))
        {
          for (int i = 1; i < 20; i++) {
            if (getConfig().isSet(str + "." + i))
            {
              String message = getConfig().getString(str.toLowerCase() + "." + i);
              sender.sendMessage(message.replace("&", "ยง"));
            }
          }
        }
        else
        {
          String message = getConfig().getString("noPermissionMessage");
          sender.sendMessage(message.replace("&", "ยง").replace("%page%", str));
        }
      }
      else
      {
        String message = getConfig().getString("pageDoesNotExistMessage");
        sender.sendMessage(message.replace("&", "ยง").replace("%page%", str));
      }
    }
    return false;
  }
}