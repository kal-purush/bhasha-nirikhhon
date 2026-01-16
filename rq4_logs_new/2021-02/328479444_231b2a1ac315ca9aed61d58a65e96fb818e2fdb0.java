package de.raidcraft.achievements.types;

import de.raidcraft.achievements.AchievementContext;
import de.raidcraft.achievements.TypeFactory;
import de.raidcraft.achievements.util.LocationUtil;
import lombok.extern.java.Log;
import org.bukkit.Location;
import org.bukkit.block.Biome;
import org.bukkit.configuration.ConfigurationSection;
import org.bukkit.entity.Player;
import org.bukkit.event.EventHandler;
import org.bukkit.event.Listener;
import org.bukkit.event.player.PlayerMoveEvent;

import java.util.*;
import java.util.stream.Collectors;

import static java.util.stream.Collectors.toMap;
import static java.util.stream.Collectors.toSet;

@Log(topic = "RCAchievements:biome")
public class BiomeAchievement extends CountAchievement implements Listener {

    public static final String VISITED_BIOMES = "visited_biomes";

    public static class Factory implements TypeFactory<BiomeAchievement> {

        @Override
        public String identifier() {

            return "biome";
        }

        @Override
        public Class<BiomeAchievement> typeClass() {

            return BiomeAchievement.class;
        }

        @Override
        public BiomeAchievement create(AchievementContext context) {

            return new BiomeAchievement(context);
        }
    }

    private final Set<Biome> biomeTypes = new HashSet<>();
    private final Map<UUID, Location> lastLocations = new HashMap<>();
    private final Map<UUID, Set<Biome>> playerVisitedBiomesMap = new HashMap<>();

    protected BiomeAchievement(AchievementContext context) {

        super(context);
    }

    @Override
    public boolean load(ConfigurationSection config) {

        biomeTypes.clear();

        for (String biome : config.getStringList("biomes")) {
            try {
                biomeTypes.add(Biome.valueOf(biome.toUpperCase()));
            } catch (IllegalArgumentException e) {
                log.warning("invalid biome name " + biome + " in config of: " + achievement());
            }
        }

        if (biomeTypes.isEmpty()) {
            biomeTypes.addAll(Arrays.asList(Biome.values()));
        }

        config.set("count", config.getInt("count", biomeTypes.size()));
        suffix(config.getString("suffix", "Biome besucht"));

        return super.load(config);
    }

    @SuppressWarnings("unchecked")
    @Override
    public void enable() {

        Map<UUID, Set<String>> entry = store().get(VISITED_BIOMES, Map.class, new HashMap<UUID, Set<String>>());
        for (Map.Entry<UUID, Set<String>> playerEntry : entry.entrySet()) {
            playerVisitedBiomesMap.put(playerEntry.getKey(), playerEntry.getValue().stream()
                    .map(Biome::valueOf)
                    .collect(Collectors.toSet())
            );
        }

        super.enable();
    }

    @Override
    public void disable() {

        store().set(VISITED_BIOMES, playerVisitedBiomesMap.entrySet().stream()
                .collect(toMap(Map.Entry::getKey, entry -> entry.getValue().stream()
                        .map(Enum::name)
                        .collect(toSet())
                ))).save();

        super.disable();
    }

    @EventHandler(ignoreCancelled = true)
    public void onPlayerMove(PlayerMoveEvent event) {

        if (notApplicable(event.getPlayer())) return;
        if (!moved(event.getPlayer())) return;
        Biome biome = event.getPlayer().getLocation().getBlock().getBiome();
        if (!biomeTypes.contains(biome)) return;

        Set<Biome> visitedBiomes = playerVisitedBiomesMap
                .computeIfAbsent(event.getPlayer().getUniqueId(), uuid -> new HashSet<>());
        if (visitedBiomes.contains(biome)) return;

        visitedBiomes.add(biome);
        playerVisitedBiomesMap.put(event.getPlayer().getUniqueId(), visitedBiomes);
        increaseAndCheck(player(event.getPlayer()));
    }

    private boolean moved(Player player) {

        Location lastLocation = lastLocations.getOrDefault(player.getUniqueId(), player.getLocation());
        lastLocations.put(player.getUniqueId(), lastLocation);
        return !LocationUtil.isBlockEquals(lastLocation, player.getLocation());
    }
}