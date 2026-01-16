/*
 * Copyright (C) 2018 Daniel Saukel
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package io.github.sataniel98.nohoe;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import org.bukkit.Material;
import org.bukkit.configuration.InvalidConfigurationException;
import org.bukkit.event.EventHandler;
import org.bukkit.event.Listener;
import org.bukkit.event.block.Action;
import org.bukkit.event.player.PlayerInteractEvent;
import org.bukkit.plugin.java.JavaPlugin;

/**
 * @author Daniel Saukel
 */
public class NoHoe extends JavaPlugin implements Listener {

    Map<Material, Short> disabled = new HashMap<>();

    @Override
    public void onEnable() {
        getDataFolder().mkdir();
        File config = new File(getDataFolder(), "config.yml");
        try {
            if (config.exists()) {
                getConfig().load(config);
            } else {
                config.createNewFile();
                getConfig().set("disabled", Arrays.asList("DIAMOND_HOE:220"));
                getConfig().save(config);
            }
        } catch (IOException | InvalidConfigurationException exception) {
            exception.printStackTrace();
        }

        for (String string : getConfig().getStringList("disabled")) {
            String[] args = string.split(":");
            Material material;
            short durability;
            try {
                material = Material.valueOf(args[0]);
            } catch (IllegalArgumentException exception) {
                continue;
            }
            try {
                durability = args.length == 2 ? Short.parseShort(args[1]) : 0;
            } catch (NumberFormatException exception) {
                continue;
            }
            disabled.put(material, durability);
        }

        getServer().getPluginManager().registerEvents(this, this);
    }

    @EventHandler
    public void onInteract(PlayerInteractEvent event) {
        if (event.getAction() != Action.RIGHT_CLICK_BLOCK) {
            return;
        }
        Material clicked = event.getClickedBlock().getType();
        if (clicked != Material.DIRT && !clicked.name().contains("GRASS") && !clicked.name().equals("GRASS_PATH")) {
            return;
        }
        Material tool = event.getItem().getType();
        short durability = event.getItem().getDurability();
        for (Entry<Material, Short> entry : disabled.entrySet()) {
            if (entry.getKey() == tool && entry.getValue() == durability) {
                event.setCancelled(true);
                return;
            }
        }
    }

}