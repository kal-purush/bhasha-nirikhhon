package de.raidcraft.achievements.types;

import de.raidcraft.achievements.TestBase;
import de.raidcraft.achievements.entities.Achievement;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

@SuppressWarnings("ALL")
class ManualCountAchievementTest extends TestBase {

    private Achievement achievement;
    private ManualCountAchievement counter;

    @Override
    @BeforeEach
    protected void setUp() {

        super.setUp();

        achievement = loadAchievement("count-test", cfg -> {
            cfg.set("type", "count");
            cfg.set("with.count", 5);
            cfg.set("childs.1.alias", "count1-child");
            cfg.set("childs.1.with.count", 10);
            cfg.set("childs.1.childs.2.alias", "count2-child");
            cfg.set("childs.1.childs.2.with.count", 20);
        }).get();
        manager().initialize(achievement);
        counter = (ManualCountAchievement) manager().active(achievement).get().type();
    }

    @Test
    @DisplayName("should increase counter of base achievement")
    void shouldIncreaseCounterOfBaseAchievement() {

        counter.count(player(), 3);

        assertThat(counter.count(player())).isEqualTo(3);
    }

    @Test
    @DisplayName("should increase counter of sub achievements")
    void shouldIncreaseCounterOfSubAchievements() {

        counter.count(player(), 3);

        assertThat(((ManualCountAchievement) manager().active("count1-child").get().type()).count(player())).isEqualTo(3);
    }

    @Test
    @DisplayName("should increase counter of sub-sub achievements")
    void shouldIncreaseCounterOfSubSubAchievements() {

        counter.count(player(), 3);

        assertThat(((ManualCountAchievement) manager().active("count2-child").get().type()).count(player())).isEqualTo(3);
    }

    @Test
    @DisplayName("should increase counter of childs after parent is unlocked")
    void shouldIncreaseCounterOfChildsAfterGoalIsReached() {

        achievement.addTo(player());

        counter.count(player(), 3);

        assertThat(((ManualCountAchievement) manager().active("count2-child").get().type()).count(player())).isEqualTo(3);
    }
}