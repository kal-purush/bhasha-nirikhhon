package io.github.milkdrinkers.stewards.gui;

import com.palmergames.bukkit.towny.TownyAPI;
import com.palmergames.bukkit.towny.object.Town;
import dev.triumphteam.gui.builder.item.ItemBuilder;
import dev.triumphteam.gui.components.GuiType;
import dev.triumphteam.gui.guis.Gui;
import io.github.alathra.alathraports.api.PortsAPI;
import io.github.milkdrinkers.colorparser.ColorParser;
import io.github.milkdrinkers.stewards.steward.Steward;
import io.github.milkdrinkers.stewards.steward.StewardLookup;
import io.github.milkdrinkers.stewards.towny.TownMetaData;
import io.github.milkdrinkers.stewards.trait.BailiffTrait;
import io.github.milkdrinkers.stewards.trait.PortmasterTrait;
import io.github.milkdrinkers.stewards.trait.StablemasterTrait;
import io.github.milkdrinkers.stewards.trait.TreasurerTrait;
import io.github.milkdrinkers.stewards.utility.Cfg;
import io.github.milkdrinkers.stewards.utility.Logger;
import net.kyori.adventure.text.Component;
import org.bukkit.Material;
import org.bukkit.entity.Player;
import org.bukkit.inventory.ItemFlag;
import org.bukkit.inventory.ItemStack;
import org.bukkit.inventory.meta.ItemMeta;

import java.util.List;

public class ConfirmFireGui {

    public static Gui createGui(Steward steward, Player player) {
        Gui gui = Gui.gui().title(Component.text("Fire " + steward.getStewardType().getName()))
            .type(GuiType.HOPPER)
            .create();

        gui.disableItemDrop()
            .disableItemPlace()
            .disableItemSwap()
            .disableItemTake();

        populateButtons(gui, steward, player);

        return gui;
    }

    private static void populateButtons(Gui gui, Steward steward, Player player) {
        ItemStack fireItem = new ItemStack(Material.PAPER);
        ItemMeta fireMeta = fireItem.getItemMeta();
        fireMeta.displayName(ColorParser.of("<green>Hire " + steward.getStewardType().getName()).build());
        fireMeta.lore(List.of(ColorParser.of("<grey>This action is permanent and cannot be undone.").build()));
        fireMeta.addItemFlags(ItemFlag.HIDE_ADDITIONAL_TOOLTIP);
        fireItem.setItemMeta(fireMeta);

        ItemStack backItem = new ItemStack(Material.PAPER);
        ItemMeta backMeta = backItem.getItemMeta();
        backMeta.displayName(ColorParser.of("<red>Back").build());
        backMeta.addItemFlags(ItemFlag.HIDE_ADDITIONAL_TOOLTIP);
        backItem.setItemMeta(backMeta);

        gui.setItem(1, 2, ItemBuilder.from(fireItem).asGuiItem(e -> {
            player.sendMessage(ColorParser.of("<green>You have fired the " + steward.getStewardType().getName() + ".").build());

            Town town = TownyAPI.getInstance().getTown(player);
            if (town == null) { // This should never happen, as player was allowed to interact with steward
                Logger.get().error("Something went wrong when checking town for {}. Town was null.", player.getName());
                return;
            }
            if (steward.getSettler().getNpc().hasTrait(BailiffTrait.class)) {

                TownMetaData.removeBailiff(town);
                town.addBonusBlocks(-1 * Cfg.get().getInt("bailiff.claims.level-" + steward.getLevel()));

            } else if (steward.getSettler().getNpc().hasTrait(PortmasterTrait.class)) {

                TownMetaData.removePortmaster(town);
                PortsAPI.deleteAbstractPort(PortsAPI.getPortFromTown(town));

            } else if (steward.getSettler().getNpc().hasTrait(StablemasterTrait.class)) {

                TownMetaData.removeStablemaster(town);
                PortsAPI.deleteAbstractCarriageStation(PortsAPI.getCarriageStationFromTown(town));

            } else if (steward.getSettler().getNpc().hasTrait(TreasurerTrait.class)) {

                TownMetaData.removeTreasurer(town);
                TownMetaData.setBankLimit(town, Cfg.get().getInt("treasurer.limit.level-0"));

            } else { // This should never happen.
                player.sendMessage(ColorParser.of("<red>Something went wrong, the steward couldn't be removed from town metadata.").build());
            }
            StewardLookup.get().unregisterSteward(steward);
            steward.getSettler().delete();
            gui.close(player);
        }));

        gui.setItem(1, 4, ItemBuilder.from(backItem).asGuiItem(e -> {
            StewardBaseGui.createBaseGui(steward, player).open(player);
        }));
    }
}