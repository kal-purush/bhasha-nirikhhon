package io.github.milkdrinkers.stewards.gui;

import dev.triumphteam.gui.builder.item.ItemBuilder;
import dev.triumphteam.gui.components.GuiType;
import dev.triumphteam.gui.guis.Gui;
import io.github.milkdrinkers.colorparser.ColorParser;
import io.github.milkdrinkers.stewards.steward.Steward;
import io.github.milkdrinkers.stewards.trait.StewardTrait;
import io.github.milkdrinkers.stewards.utility.Appearance;
import net.citizensnpcs.api.npc.NPC;
import net.citizensnpcs.trait.SkinTrait;
import net.kyori.adventure.text.Component;
import org.bukkit.Location;
import org.bukkit.Material;
import org.bukkit.entity.Player;
import org.bukkit.inventory.ItemFlag;
import org.bukkit.inventory.ItemStack;
import org.bukkit.inventory.meta.ItemMeta;

public class AppearanceGui {

    public static Gui createGui(Steward steward, Player player) {
        Gui gui = Gui.gui().title(Component.text("Re-roll name and skin")) // TODO FIX THIS SHIT
            .rows(1)
            .create();

        gui.disableItemDrop()
            .disableItemPlace()
            .disableItemSwap()
            .disableItemTake();

        populateButtons(gui, steward, player);

        return gui;
    }

    private static void populateButtons(Gui gui, Steward steward, Player player) {
        boolean female = steward.getSettler().getNpc().getOrAddTrait(StewardTrait.class).isFemale();

        ItemStack exitItem = new ItemStack(Material.PAPER);
        ItemMeta exitMeta = exitItem.getItemMeta();
        exitMeta.displayName(ColorParser.of("<red>Back").build());
        exitMeta.addItemFlags(ItemFlag.HIDE_ADDITIONAL_TOOLTIP);
        exitItem.setItemMeta(exitMeta);

        gui.setItem(1, 9, ItemBuilder.from(exitItem).asGuiItem(event -> StewardBaseGui.createBaseGui(steward, player).open(player)));

        ItemStack nameItem = new ItemStack(Material.PAPER); // TODO: Placeholder item
        ItemMeta nameMeta = nameItem.getItemMeta();
        nameMeta.displayName(ColorParser.of("<green>Re-roll name").build());
        nameMeta.addItemFlags(ItemFlag.HIDE_ADDITIONAL_TOOLTIP);
        nameItem.setItemMeta(nameMeta);

        ItemStack skinItem = new ItemStack(Material.PAPER); // TODO: Placeholder item
        ItemMeta skinMeta = skinItem.getItemMeta();
        skinMeta.displayName(ColorParser.of("<green>Re-roll skin").build());
        skinMeta.addItemFlags(ItemFlag.HIDE_ADDITIONAL_TOOLTIP);
        skinItem.setItemMeta(skinMeta);

        ItemStack nameAndSkinItem = new ItemStack(Material.PAPER); // TODO: Placeholder item
        ItemMeta nameAndSkinMeta = nameAndSkinItem.getItemMeta();
        nameAndSkinMeta.displayName(ColorParser.of("<green>Re-roll name and skin!").build());
        nameAndSkinMeta.addItemFlags(ItemFlag.HIDE_ADDITIONAL_TOOLTIP);
        nameAndSkinItem.setItemMeta(nameAndSkinMeta);

        gui.setItem(1, 3, ItemBuilder.from(nameItem).asGuiItem(e -> {
            String newName;
            if (female) {
                newName = Appearance.getFemaleName();
                steward.getSettler().getNpc().setName(newName);
                steward.getSettler().getNpc().getEntity().customName(Component.text(newName));
                steward.getSettler().getNpc().getEntity().setCustomNameVisible(true);

                Location location = steward.getSettler().getNpc().getEntity().getLocation();
                steward.getSettler().getNpc().despawn();
                steward.getSettler().getNpc().spawn(location);
            } else {
                newName = Appearance.getMaleName();
                steward.getSettler().getNpc().setName(newName);
                steward.getSettler().getNpc().data().setPersistent(NPC.Metadata.NAMEPLATE_VISIBLE, true);

                Location location = steward.getSettler().getNpc().getEntity().getLocation();
                steward.getSettler().getNpc().despawn();
                steward.getSettler().getNpc().spawn(location);
            }
            gui.close(player);
        }));

        gui.setItem(1, 5, ItemBuilder.from(skinItem).asGuiItem(e -> {
            if (female) {
                Appearance.applyFemaleStewardSkin(steward);
            } else {
                Appearance.applyMaleStewardSkin(steward);
            }
            gui.close(player);
        }));

        gui.setItem(1, 7, ItemBuilder.from(nameAndSkinItem).asGuiItem(e -> {
            boolean femaleNew = Math.random() > 0.5;
            String newName;

            if (femaleNew) {
                Appearance.applyFemaleStewardSkin(steward);

                newName = Appearance.getFemaleName();
                steward.getSettler().getNpc().setName(newName);
                steward.getSettler().getNpc().getEntity().customName(Component.text(newName));
                steward.getSettler().getNpc().getEntity().setCustomNameVisible(true);

                steward.getSettler().getNpc().getOrAddTrait(StewardTrait.class).setFemale(true);
            } else {
                Appearance.applyMaleStewardSkin(steward);

                newName = Appearance.getMaleName();
                steward.getSettler().getNpc().setName(newName);
                steward.getSettler().getNpc().getEntity().customName(Component.text(newName));
                steward.getSettler().getNpc().getEntity().setCustomNameVisible(true);

                steward.getSettler().getNpc().getOrAddTrait(StewardTrait.class).setFemale(false);
            }
            gui.close(player);
        }));
    }

}