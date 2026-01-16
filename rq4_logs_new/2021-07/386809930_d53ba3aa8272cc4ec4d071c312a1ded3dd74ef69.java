package com.github.ckblck.armor.tracker.calculation;

import com.github.ckblck.armor.tracker.calculation.piece.ArmorPiece;
import lombok.Data;
import lombok.RequiredArgsConstructor;
import org.bukkit.inventory.ItemStack;

/**
 * Data class that holds
 * the method and the modified armors
 * of an armor equip event.
 * <p>
 * Think it this way:
 * A player's armor contents might be modified
 * affecting more than one slot ({@link org.bukkit.inventory.PlayerInventory#setArmorContents(ItemStack[])}).
 * That explains why {@link #modifiedArmor} is an array.
 */

@Data
@RequiredArgsConstructor
public class ArmorModification {
    private final MethodCalculator.Method modificationMethod;
    private final ArmorPiece[] modifiedArmor;
}