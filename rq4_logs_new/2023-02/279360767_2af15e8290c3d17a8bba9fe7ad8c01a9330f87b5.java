package net.zeeraa.novacore.spigot.abstraction.manager;

import net.zeeraa.novacore.commons.utils.ClassFinder;
import net.zeeraa.novacore.spigot.abstraction.VersionIndependentUtils;
import net.zeeraa.novacore.spigot.abstraction.packet.event.AsyncPacketEvent;
import org.bukkit.entity.Player;
import org.bukkit.event.Event;
import org.bukkit.event.player.AsyncPlayerChatEvent;
import org.bukkit.event.player.PlayerInteractEvent;
import org.bukkit.event.player.PlayerItemHeldEvent;
import org.bukkit.event.player.PlayerMoveEvent;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

public class CustomSpectatorManager {

    private static List<Player> spectators;
    private static List<Class<? extends Event>> permittedEvents;

    static {
        spectators = new ArrayList<>();
        permittedEvents = new ArrayList<>();
        permittedEvents.add(AsyncPlayerChatEvent.class);
        permittedEvents.add(PlayerMoveEvent.class);
        permittedEvents.add(PlayerInteractEvent.class);
        permittedEvents.add(PlayerItemHeldEvent.class);
    }


    public static void addPermittedEvent(Class<? extends Event> event) {
        if (!permittedEvents.contains(event)) {
            permittedEvents.add(event);
        }
    }

    public static void removePermittedEvent(Class<? extends Event> event) {
        if (permittedEvents.contains(event)) {
            permittedEvents.remove(event);
        }
    }

    public static List<Class<? extends Event>> getPermittedEvents() {
        return permittedEvents;
    }

    public static void setSpectator(Player player, boolean value) {
        VersionIndependentUtils.get().setCustomSpectator(player, value);
    }

    public static void setSpectator(Player player, boolean value, Collection<? extends Player> players) {
        VersionIndependentUtils.get().setCustomSpectator(player, value, players);
    }

    public static boolean isSpectator(Player player) {
        return spectators.contains(player);
    }

    public static List<Player> getSpectators() {
        return spectators;
    }


}