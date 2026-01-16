package de.raidcraft.achievements;

import de.raidcraft.achievements.entities.PlacedBlock;
import de.raidcraft.achievements.util.TimeUtil;
import io.ebean.Model;
import io.ebean.Transaction;
import lombok.Synchronized;
import org.bukkit.Bukkit;
import org.bukkit.block.Block;
import org.bukkit.event.EventHandler;
import org.bukkit.event.EventPriority;
import org.bukkit.event.Listener;
import org.bukkit.event.block.*;
import org.bukkit.scheduler.BukkitTask;

import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.*;

public class BlockTracker implements Listener {

    private final RCAchievements plugin;
    private final Set<PlacedBlock> placedBlocks = new HashSet<>();
    private final Set<PlacedBlock> destroyedBlocks = Collections.synchronizedSet(new HashSet<>());
    private BukkitTask saveTask;

    BlockTracker(RCAchievements plugin) {
        this.plugin = plugin;
    }

    void enable() {

        placedBlocks.clear();
        placedBlocks.addAll(PlacedBlock.find.all());

        saveTask = Bukkit.getScheduler().runTaskTimerAsynchronously(
                plugin,
                this::saveData,
                plugin.pluginConfig().getSaveBlockPlacementsInterval(),
                plugin.pluginConfig().getSaveBlockPlacementsInterval()
        );
    }

    void disable() {

        if (saveTask != null) {
            saveTask.cancel();
        }
        saveData();
        placedBlocks.clear();
        destroyedBlocks.clear();
    }

    public boolean isPlayerPlacedBlock(Block block) {

        return placedBlocks.contains(new PlacedBlock(block));
    }

    @EventHandler(ignoreCancelled = true, priority = EventPriority.MONITOR)
    public void onBlockPlace(BlockPlaceEvent event) {

        placedBlocks.add(new PlacedBlock(event.getBlock(), event.getPlayer().getUniqueId()));
    }

    @EventHandler(ignoreCancelled = true, priority = EventPriority.MONITOR)
    public void onBlockBreak(BlockBreakEvent event) {

        PlacedBlock block = new PlacedBlock(event.getBlock());
        if (placedBlocks.remove(block)) {
            destroyedBlocks.add(block);
        }
    }

    @EventHandler(ignoreCancelled = true, priority = EventPriority.MONITOR)
    public void onBlockMove(BlockPistonExtendEvent event) {

        for (Block block : event.getBlocks()) {
            if (placedBlocks.remove(new PlacedBlock(block))) {
                placedBlocks.add(new PlacedBlock(block.getRelative(event.getDirection())));
            }
        }
    }

    @EventHandler(ignoreCancelled = true, priority = EventPriority.MONITOR)
    public void onBlockMove(BlockPistonRetractEvent event) {

        for (Block block : event.getBlocks()) {
            if (placedBlocks.remove(new PlacedBlock(block))) {
                placedBlocks.add(new PlacedBlock(block.getRelative(event.getDirection())));
            }
        }
    }

    @Synchronized
    private Set<PlacedBlock> getAndClearDestroyedBlocks() {

        synchronized (destroyedBlocks) {
            Set<PlacedBlock> blocks = Set.copyOf(destroyedBlocks);
            destroyedBlocks.clear();
            return blocks;
        }
    }

    private void saveData() {

        try (Transaction transaction = plugin.database().beginTransaction()) {

            getAndClearDestroyedBlocks().stream()
                    .map(PlacedBlock::from)
                    .flatMap(Optional::stream)
                    .forEach(Model::delete);

            transaction.commitAndContinue();

            Set.copyOf(this.placedBlocks).forEach(Model::save);
            transaction.commitAndContinue();

            PlacedBlock.find.query()
                    .where().lt("when_created", Instant.now()
                    .minus(TimeUtil.parseTimeAsMilliseconds(plugin.pluginConfig().getBlockPlacementCacheTime()), ChronoUnit.MILLIS))
                    .delete();
            transaction.commit();
        }
    }
}