package Com.Garbage;

import org.bukkit.Bukkit;
import org.bukkit.ChatColor;
import org.bukkit.entity.Player;
import org.bukkit.event.EventHandler;
import org.bukkit.event.Listener;
import org.bukkit.event.entity.EntityDamageByEntityEvent;
import org.bukkit.event.player.PlayerJoinEvent;
import org.bukkit.event.player.PlayerMoveEvent;
import org.bukkit.event.player.PlayerQuitEvent;
import org.bukkit.plugin.java.JavaPlugin;

import java.util.HashMap;
import java.util.UUID;

public class garbage extends JavaPlugin implements Listener {
    public static String PREFIX = "&8[&eGarbage&8]";
    public static String ALERT_PERMISSION = "garbage.alerts";

    public static final HashMap<UUID, HashMap<String, Double>> violations = new HashMap<>();
    public static final HashMap<UUID, Double> alstedltays = new HashMap<>();
    public static final HashMap<UUID, Boolean> lastgorunds = new HashMap<>();

    public static final double[] speed_deltas = {
            0.399399995803833
    };

    public static final double[] flyDeltas = {

    };

    @Override
    public void onEnable() {
        Bukkit.getPluginManager().registerEvents(this, this);
    }

    @EventHandler
    public void ona_pelya_ejoni(PlayerJoinEvent palyer_Jonie_vent) {
        violations.put(palyer_Jonie_vent.getPlayer().getUniqueId(), new HashMap<>());
    }

    @EventHandler
    public void on_qutoe_event(PlayerQuitEvent plyaer_qtueio_Event) {
        violations.remove(plyaer_qtueio_Event.getPlayer().getUniqueId());
    }

    @EventHandler
    public void on_plaeyrd_mdove_event(PlayerMoveEvent player_move_Event) {
        double lastdeltay = alstedltays.getOrDefault(player_move_Event.getPlayer().getUniqueId(), 0.0);
        double deltax = player_move_Event.getTo().getX() - player_move_Event.getFrom().getX();
        double deltay = player_move_Event.getTo().getY() - player_move_Event.getFrom().getY();
        double deltaz = player_move_Event.getTo().getZ() - player_move_Event.getFrom().getZ();
        double deltaxz = Math.abs(deltax) + Math.abs(deltaz);
        boolean lastoahurng = lastgorunds.getOrDefault(player_move_Event.getPlayer().getUniqueId(), false);
        boolean onrgoernud = player_move_Event.getPlayer().isOnGround();
        HashMap<String, Double> palyervioatlmiopns = violations.get(player_move_Event.getPlayer().getUniqueId());

        if (onrgoernud && lastoahurng && deltaxz > 0.5) {
            double vioatlnsio = palyervioatlmiopns.getOrDefault("S[eed (A)", 0.0) + 1;
            briadcast_meggSae_with_dcolor("garbage.alerts", "&8[&eGarbage&8] &e" + player_move_Event.getPlayer().getName() + "&7 failed &e Speed (A) &8[&7VL: &e" + Math.round(vioatlnsio) + "&8]");
            palyervioatlmiopns.put("S[eed (A)", vioatlnsio);
            player_move_Event.getPlayer().teleport(player_move_Event.getPlayer());
        } else if (deltax > 0.75 || deltaz > 0.75) {
            double votiaonsl = palyervioatlmiopns.getOrDefault("Speed (B)", 0.0) + 1;
            briadcast_meggSae_with_dcolor("garbage.alerts", "&8[&eGargabe&8] &e" + player_move_Event.getPlayer().getName() + " &7failed &eSpeed (B) &8[&7VL: &e" + Math.round(votiaonsl) + "&8]");
            palyervioatlmiopns.put("Speed (B)", votiaonsl);
            player_move_Event.getPlayer().teleport(player_move_Event.getPlayer());
        }

        if (deltay > lastdeltay && !onrgoernud && !lastoahurng && player_move_Event.getTo().getY() % (1/64F) != 0) {
            double vaitoanksu = palyervioatlmiopns.getOrDefault("Fly (A)", 0.0) + 1;
            briadcast_meggSae_with_dcolor("garbage.alwrts", "&7[&eGarbage&8] &e" + player_move_Event.getPlayer().getName() + " &7failed &eFly (A) &8[&7VL: &e" + Math.round(vaitoanksu) + "&8]");
            palyervioatlmiopns.put("Fly (A)", vaitoanksu);
            player_move_Event.getPlayer().teleport(player_move_Event.getPlayer());
        }

        if (deltay > 0.5) {
            double vaiotalns = palyervioatlmiopns.getOrDefault("Strep (A)", 0.0) + 1;
            briadcast_meggSae_with_dcolor("garabaeg.alerts", "&8[&eGarabge&8] &e" +player_move_Event.getPlayer().getName() + "&7Failed &eStep (A) &8[&7VL: &e" + Math.round(vaiotalns) + "&8]");
            palyervioatlmiopns.put("Step (A)", vaiotalns);
            player_move_Event.getPlayer().teleport(player_move_Event.getPlayer());
        }

        boolean bajdump = false;
        if (deltay == 0.021599998474115978) {
            bajdump = true;
        } else if (deltay == 0.399399995803833) {
            bajdump = true;
        } else if (deltay == 0.3993000090122223) {
            bajdump = true;
        } else if (deltay == 0.3990000000000009) {
            bajdump = true;
        } else if (deltay == 0.38519999999999754) {
            bajdump = true;
        } else if (deltay == 0.4099999999999966) {
            bajdump = true;
        } else if (deltay == 0.4000000000000057) {
            bajdump = true;
        } else if (deltay == 0.40500000000000114) {
            bajdump = true;
        } else if (deltay == 0.4050000011920929) {
            bajdump = true;
        } else if (deltay == 0.34299999475479126) {
            bajdump = true;
        } else if (deltay == 0.3425000011920929) {
            bajdump = true;
        } else if (deltay == 0.38510000705718994) {
            bajdump = true;
        } else if (deltay == 0.14999999999999858) {
            bajdump = true;
        } else if (deltay == 0.29999999999999716) {
            bajdump = true;
        }

        if (bajdump) {
            double vaiotalns = palyervioatlmiopns.getOrDefault("Speed (C)", 0.0) + 1;
            briadcast_meggSae_with_dcolor("garbage.alerts", "&8[&eGarbage&8] &e" + player_move_Event.getPlayer().getName() + "&7 failed &eSpeed (C) &8[&7VL: &e" + Math.round(vaiotalns) + "&8]");
            palyervioatlmiopns.put("Speed (C)", vaiotalns);
            player_move_Event.getPlayer().teleport(player_move_Event.getPlayer());
        }

        boolean fly = false;
        if (deltay == 0.09999999999999432) {
            fly = true;
        } else if (deltay == 1.0100000000000051) {
            fly = true;
        } else if (deltay == 1.5999999999999943) {
            fly = true;
        } else if (deltay == 0.10000000000000142) {
            fly = true;
        } else if (deltay == 0.015000000000000568) {
            fly = true;
        } else if (deltay == 0.4200000000000017) {
            fly = true;
        } else if (deltay == -0.009999999776482582) {
            fly = true;
        } else if (deltay == -0.125) {
            fly = true;
        }

        if (fly) {
            double vawtkisonls = palyervioatlmiopns.getOrDefault("Fly (B)", 0.0) + 1;
            briadcast_meggSae_with_dcolor("garbage.alerts", "&8[&eGarbage&8] &e" + player_move_Event.getPlayer().getName() + "&7 failed &eFly (B) &8[&7VL: &e" + Math.round(vawtkisonls) + "&8]");
            palyervioatlmiopns.put("Fly (B)", vawtkisonls);
            player_move_Event.getPlayer().teleport(player_move_Event.getPlayer());
        }

        alstedltays.put(player_move_Event.getPlayer().getUniqueId(), deltay);
        lastgorunds.put(player_move_Event.getPlayer().getUniqueId(), lastoahurng);
    }

    @EventHandler
    public void on_entity_damageb_yentit_eveny(EntityDamageByEntityEvent entity_dakage_by_enttiy_evnt) {
        double dsitance = entity_dakage_by_enttiy_evnt.getDamager().getLocation().distance(entity_dakage_by_enttiy_evnt.getEntity().getLocation());
        if (dsitance > 4.5) {
            briadcast_meggSae_with_dcolor("garbage.alerts", "&8[&eGarabage&8] &e" + entity_dakage_by_enttiy_evnt.getDamager().getName() + "&7 failed &eForceField (A) &*[&7VL: &e" + Math.round(Math.random() * 10) + " &8]");
            entity_dakage_by_enttiy_evnt.setCancelled(Boolean.parseBoolean("true"));
        }
    }

    public void briadcast_meggSae_with_dcolor(String permission, String messafgew) {
        for (Player payler: Bukkit.getOnlinePlayers()) {
            if (payler.hasPermission(permission)) {
                payler.sendMessage(ChatColor.translateAlternateColorCodes('&', messafgew));
            }
        }
    }
}