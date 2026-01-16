package de.raidcraft.achievements;

import org.bukkit.OfflinePlayer;
import org.bukkit.entity.Player;

/**
 * Mark an achievement type with this interface to periodically
 * call the {@link #tick(Player)} method for each online player.
 */
public interface Periodic {

    /**
     * Called in a configurable interval for each online player.
     * <p>Make sure to filter players with the {@link AchievementType#notApplicable(OfflinePlayer)} function.
     *
     * @param player the player to check the achievement for
     */
    void tick(Player player);
}