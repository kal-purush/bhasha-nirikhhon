package com.simibubi.create.content.logistics.packager;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.compress.utils.Lists;

import com.simibubi.create.content.logistics.box.PackageItem;
import com.simibubi.create.content.logistics.stockTicker.PackageOrder;

import net.createmod.catnip.utility.IntAttached;
import net.minecraft.nbt.CompoundTag;
import net.minecraft.world.item.ItemStack;
import net.minecraftforge.items.ItemHandlerHelper;
import net.minecraftforge.items.ItemStackHandler;

public class PackageDefragmenter {

	protected Map<Integer, List<ItemStack>> collectedPackages = new HashMap<>();

	public void clear() {
		collectedPackages.clear();
	}

	public boolean isFragmented(ItemStack box) {
		if (!box.hasTag() || !box.getTag()
			.contains("Fragment"))
			return false;

		CompoundTag fragTag = box.getTag()
			.getCompound("Fragment");

		return !(fragTag.getInt("LinkIndex") == 0 && fragTag.getBoolean("IsFinalLink") && fragTag.getInt("Index") == 0
			&& fragTag.getBoolean("IsFinal"));
	}

	public int addPackageFragment(ItemStack box) {
		int collectedOrderId = PackageItem.getOrderId(box);
		if (collectedOrderId == -1)
			return -1;
		
		List<ItemStack> collectedOrder = collectedPackages.computeIfAbsent(collectedOrderId, $ -> Lists.newArrayList());
		collectedOrder.add(box);

		if (!isOrderComplete(collectedOrderId))
			return -1;

		return collectedOrderId;
	}

	public List<ItemStack> repack(int orderId) {
		List<ItemStack> exportingPackages = new ArrayList<>();
		String address = "";
		PackageOrder order = null;
		List<IntAttached<ItemStack>> allItems = new ArrayList<>();

		for (ItemStack box : collectedPackages.get(orderId)) {
			address = PackageItem.getAddress(box);
			if (box.hasTag() && box.getTag()
				.getCompound("Fragment")
				.contains("OrderContext"))
				order = PackageOrder.read(box.getTag()
					.getCompound("Fragment")
					.getCompound("OrderContext"));
			ItemStackHandler contents = PackageItem.getContents(box);
			Slots: for (int slot = 0; slot < contents.getSlots(); slot++) {
				ItemStack stackInSlot = contents.getStackInSlot(slot);
				for (IntAttached<ItemStack> existing : allItems) {
					if (!ItemHandlerHelper.canItemStacksStack(stackInSlot, existing.getValue()))
						continue;
					existing.setFirst(existing.getFirst() + stackInSlot.getCount());
					continue Slots;
				}
				allItems.add(IntAttached.with(stackInSlot.getCount(), stackInSlot));
			}
		}

		List<IntAttached<ItemStack>> orderedStacks = order == null ? Collections.emptyList() : order.stacks();
		List<ItemStack> outputSlots = new ArrayList<>();

		Repack: while (true) {
			allItems.removeIf(e -> e.getFirst() == 0);
			if (allItems.isEmpty())
				break;

			IntAttached<ItemStack> targetedEntry = null;
			if (!orderedStacks.isEmpty())
				targetedEntry = orderedStacks.remove(0);

			ItemSearch: for (IntAttached<ItemStack> entry : allItems) {
				int targetAmount = entry.getFirst();
				if (targetAmount == 0)
					continue;
				if (targetedEntry != null) {
					targetAmount = targetedEntry.getFirst();
					if (!ItemHandlerHelper.canItemStacksStack(entry.getSecond(), targetedEntry.getSecond()))
						continue;
				}

				while (targetAmount > 0) {
					int removedAmount = Math.min(Math.min(targetAmount, entry.getSecond()
						.getMaxStackSize()), entry.getFirst());
					if (removedAmount == 0)
						continue ItemSearch;

					ItemStack output = ItemHandlerHelper.copyStackWithSize(entry.getSecond(), removedAmount);
					targetAmount -= removedAmount;
					targetedEntry.setFirst(targetAmount);
					entry.setFirst(entry.getFirst() - removedAmount);
					outputSlots.add(output);
				}

				continue Repack;
			}
		}

		int currentSlot = 0;
		ItemStackHandler target = new ItemStackHandler(PackageItem.SLOTS);

		for (ItemStack item : outputSlots) {
			target.setStackInSlot(currentSlot++, item);
			if (currentSlot < PackageItem.SLOTS)
				continue;
			exportingPackages.add(PackageItem.containing(target));
			target = new ItemStackHandler(PackageItem.SLOTS);
			currentSlot = 0;
		}

		exportingPackages.add(PackageItem.containing(target));

		for (ItemStack box : exportingPackages)
			PackageItem.addAddress(box, address);
		
		return exportingPackages;
	}

	private boolean isOrderComplete(int orderId) {
		boolean finalLinkReached = false;
		Links: for (int linkCounter = 0; linkCounter < 1000; linkCounter++) {
			if (finalLinkReached)
				break;
			Packages: for (int packageCounter = 0; packageCounter < 1000; packageCounter++) {
				for (ItemStack box : collectedPackages.get(orderId)) {
					CompoundTag tag = box.getOrCreateTag()
						.getCompound("Fragment");
					if (linkCounter != tag.getInt("LinkIndex"))
						continue;
					if (packageCounter != tag.getInt("Index"))
						continue;
					finalLinkReached = tag.getBoolean("IsFinalLink");
					if (tag.getBoolean("IsFinal"))
						continue Links;
					continue Packages;
				}
				return false;
			}
		}
		return true;
	}

}