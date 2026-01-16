package de.raidcraft.achievements.types;

import de.raidcraft.achievements.AchievementContext;
import de.raidcraft.achievements.TestBase;
import de.raidcraft.achievements.entities.DataStore;
import lombok.SneakyThrows;
import org.bukkit.configuration.MemoryConfiguration;
import org.bukkit.event.player.PlayerLoginEvent;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.time.Instant;
import java.time.temporal.ChronoUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.*;

class LoginAchievementTest extends TestBase {

    private DataStore store;
    private LoginAchievement achievement;
    private AchievementContext context;

    @SneakyThrows
    @BeforeEach
    protected void setUp() {

        super.setUp();

        store = new DataStore();

        context = mock(AchievementContext.class);
        when(context.store(player())).thenReturn(store);
        when(context.player(bukkitPlayer())).thenReturn(player());
        achievement = new LoginAchievement(context);
    }

    @Nested
    @DisplayName("onLogin()")
    class OnLogin {

        @SneakyThrows
        @Test
        @DisplayName("should increase count on first login")
        void shouldIncreaseCountOnFirstLogin() {

            MemoryConfiguration config = new MemoryConfiguration();
            config.set("count", 5);

            achievement.load(config);

            achievement.onLogin(new PlayerLoginEvent(bukkitPlayer(), "", InetAddress.getByName("localhost")));

            assertThat(achievement.count(player())).isEqualTo(1);
        }

        @SneakyThrows
        @Test
        @DisplayName("should reset count to one after failed streak")
        void shouldResetCountAfterFailedLogin() {

            MemoryConfiguration config = new MemoryConfiguration();
            config.set("count", 5);

            achievement.load(config);

            store.set(LoginAchievement.LAST_LOGIN, Instant.now().minus(2, ChronoUnit.DAYS).toEpochMilli());
            store.set(CountAchievement.COUNT_KEY, 4);

            achievement.onLogin(new PlayerLoginEvent(bukkitPlayer(), "", InetAddress.getByName("localhost")));

            assertThat(achievement.count(player())).isEqualTo(1);
        }

        @Test
        @DisplayName("should give achievement ot player when count is reached")
        void shouldGiveAchievementWhenCountIsMet() throws UnknownHostException {

            MemoryConfiguration config = new MemoryConfiguration();
            config.set("count", 1);

            achievement.load(config);

            achievement.onLogin(new PlayerLoginEvent(bukkitPlayer(), "", InetAddress.getByName("localhost")));

            verify(context, times(1)).addTo(player());
        }

        @Test
        @DisplayName("should only increase the counter once per day")
        void shouldOnlyIncreaseTheCounterOncePerDay() throws UnknownHostException {

            MemoryConfiguration config = new MemoryConfiguration();
            config.set("count", 5);

            achievement.load(config);

            achievement.onLogin(new PlayerLoginEvent(bukkitPlayer(), "", InetAddress.getByName("localhost")));
            achievement.onLogin(new PlayerLoginEvent(bukkitPlayer(), "", InetAddress.getByName("localhost")));
            achievement.onLogin(new PlayerLoginEvent(bukkitPlayer(), "", InetAddress.getByName("localhost")));

            assertThat(achievement.count(player())).isEqualTo(1);
        }
    }

    @Nested
    @DisplayName("today()")
    class Today {

        @Test
        @DisplayName("should return true if last check was today")
        void shouldReturnTrueIfLastCheckWasToday() {

            achievement.checkedToday.add(player().id());

            assertThat(achievement.today(bukkitPlayer())).isTrue();
        }

        @Test
        @DisplayName("should check last login of player if cache is empty")
        void shouldCheckLastLoginOfPlayerIfCacheIsEmpty() {

            store.set(LoginAchievement.LAST_LOGIN, Instant.now().toEpochMilli()).save();

            assertThat(achievement.today(bukkitPlayer())).isTrue();
        }

        @Test
        @DisplayName("should return false if not logged in or checked today")
        void shouldReturnFalseIfNotCheckedToday() {

            assertThat(achievement.today(bukkitPlayer())).isFalse();
        }

        @Test
        @DisplayName("should return false if last login was yesterday")
        void shouldReturnFalseIfLastLoginWasYesterday() {

            store.set(LoginAchievement.LAST_LOGIN, Instant.now().minus(1, ChronoUnit.DAYS).toEpochMilli()).save();

            assertThat(achievement.today(bukkitPlayer())).isFalse();
        }
    }

    @Nested
    @DisplayName("streak()")
    class Streak {

        @Test
        @DisplayName("should have a streak on the first day")
        void shouldHaveAStreakOnTheFirstDay() {

            assertThat(achievement.streak(bukkitPlayer())).isTrue();
        }

        @Test
        @DisplayName("should have a streak on the second login day in a row")
        void shouldHaveStreakOnSecondDay() {

            store.set(LoginAchievement.LAST_LOGIN, Instant.now().minus(1, ChronoUnit.DAYS).toEpochMilli()).save();

            assertThat(achievement.streak(bukkitPlayer())).isTrue();
        }

        @Test
        @DisplayName("should reset streak on missed day")
        void shouldResetStreakOnMissedDay() {

            store.set(LoginAchievement.LAST_LOGIN, Instant.now().minus(2, ChronoUnit.DAYS).toEpochMilli()).save();

            assertThat(achievement.streak(bukkitPlayer())).isFalse();
        }
    }
}