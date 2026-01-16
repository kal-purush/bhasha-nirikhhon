package de.raidcraft.achievements.events;

import de.raidcraft.achievements.entities.PlayerAchievement;
import lombok.Getter;
import org.bukkit.event.HandlerList;

/**
 * The PlayerLostAchievementEvent is fired after the player lost an achievement.
 */
public class PlayerLostAchievementEvent extends PlayerAchievementEvent {

    @Getter
    private static final HandlerList handlerList = new HandlerList();

    public PlayerLostAchievementEvent(PlayerAchievement playerAchievement) {

        super(playerAchievement);
    }

    @Override
    public HandlerList getHandlers() {

        return handlerList;
    }
}