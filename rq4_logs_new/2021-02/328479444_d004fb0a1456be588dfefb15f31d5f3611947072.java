package de.raidcraft.achievements.types;

import de.raidcraft.achievements.AbstractAchievementType;
import de.raidcraft.achievements.AchievementContext;
import de.raidcraft.achievements.Progressable;
import de.raidcraft.achievements.TypeFactory;
import de.raidcraft.achievements.entities.AchievementPlayer;
import de.raidcraft.achievements.events.AchievementProgressChangeEvent;
import de.raidcraft.economy.events.PlayerBalanceChangedEvent;
import de.raidcraft.economy.wrapper.Economy;
import net.kyori.adventure.text.Component;
import org.bukkit.Bukkit;
import org.bukkit.configuration.ConfigurationSection;
import org.bukkit.event.EventHandler;
import org.bukkit.event.Listener;

import static de.raidcraft.achievements.Messages.Colors.*;
import static net.kyori.adventure.text.Component.text;

public class MoneyAchievement extends AbstractAchievementType implements Listener, Progressable {

    public static class Factory implements TypeFactory<MoneyAchievement> {

        @Override
        public String identifier() {

            return "money";
        }
        @Override
        public Class<MoneyAchievement> typeClass() {
            return MoneyAchievement.class;
        }

        @Override
        public MoneyAchievement create(AchievementContext context) {
            return new MoneyAchievement(context);
        }

    }
    private double amount;

    protected MoneyAchievement(AchievementContext context) {
        super(context);
    }

    @Override
    public boolean load(ConfigurationSection config) {

        this.amount = config.getDouble("amount");

        return true;
    }

    @Override
    public Component progressText(AchievementPlayer player) {

        Economy economy = Economy.get();
        return text("Geld: ", TEXT)
                .append(text(economy.format(economy.getBalance(player.offlinePlayer())), DARK_HIGHLIGHT))
                .append(text("/", ACCENT))
                .append(text(economy.format(amount), HIGHLIGHT))
                .append(text(" " + economy.currencyNamePlural(), TEXT));
    }

    @Override
    public float progress(AchievementPlayer player) {

        float progress = (float) (Economy.get().getBalance(player.offlinePlayer()) / amount);
        return Math.min(progress, 1.0f);
    }

    @EventHandler(ignoreCancelled = true)
    public void onBalanceChange(PlayerBalanceChangedEvent event) {

        AchievementPlayer player = AchievementPlayer.of(event.getPlayer());
        Bukkit.getPluginManager().callEvent(new AchievementProgressChangeEvent(
                playerAchievement(player),
                this
        ));

        if (Economy.get().getBalance(player.offlinePlayer()) >= amount) {
            addTo(player);
        }
    }
}