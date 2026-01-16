package de.raidcraft.achievements.types;

import de.raidcraft.achievements.AchievementContext;
import de.raidcraft.achievements.TypeFactory;
import org.bukkit.event.Listener;

import java.time.Instant;
import java.util.HashSet;
import java.util.Set;
import java.util.UUID;

public class ManualCountAchievement extends CountAchievement implements Listener {

    public static class Factory implements TypeFactory<ManualCountAchievement> {

        @Override
        public String identifier() {

            return "count";
        }

        @Override
        public Class<ManualCountAchievement> typeClass() {

            return ManualCountAchievement.class;
        }

        @Override
        public ManualCountAchievement create(AchievementContext context) {

            return new ManualCountAchievement(context);
        }
    }

    protected ManualCountAchievement(AchievementContext context) {

        super(context);
    }
}