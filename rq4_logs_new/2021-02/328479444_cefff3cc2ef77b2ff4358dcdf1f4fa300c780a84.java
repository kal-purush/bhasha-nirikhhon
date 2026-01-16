package de.raidcraft.achievements.events;

import de.raidcraft.achievements.entities.PlayerAchievement;
import de.raidcraft.achievements.types.CountAchievement;
import lombok.Getter;
import lombok.NonNull;
import org.bukkit.event.HandlerList;
import org.jetbrains.annotations.NotNull;

public class AchievementCountChangedEvent extends PlayerAchievementEvent {

    @Getter
    private static final HandlerList handlerList = new HandlerList();

    @Getter
    private final CountAchievement countAchievement;
    private final long count;

    public AchievementCountChangedEvent(@NotNull PlayerAchievement playerAchievement, @NonNull CountAchievement countAchievement, long count) {

        super(playerAchievement);
        this.countAchievement = countAchievement;
        this.count = count;
    }

    @NotNull
    @Override
    public HandlerList getHandlers() {

        return handlerList;
    }
}