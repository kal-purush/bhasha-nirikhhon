package de.raidcraft.achievements.events;

import de.raidcraft.achievements.Progressable;
import de.raidcraft.achievements.entities.PlayerAchievement;
import lombok.Getter;
import lombok.NonNull;
import net.kyori.adventure.text.Component;
import org.bukkit.event.HandlerList;
import org.jetbrains.annotations.NotNull;

public class AchievementProgressChangeEvent extends PlayerAchievementEvent {

    @Getter
    private static final HandlerList handlerList = new HandlerList();

    @Getter
    private final Progressable progressable;

    public AchievementProgressChangeEvent(@NotNull PlayerAchievement playerAchievement, @NonNull Progressable progressable) {

        super(playerAchievement);
        this.progressable = progressable;
    }

    public float progress() {

        return progressable.progress(player());
    }

    public Component progressText() {

        return progressable.progressText(player());
    }

    @NotNull
    @Override
    public HandlerList getHandlers() {

        return handlerList;
    }
}