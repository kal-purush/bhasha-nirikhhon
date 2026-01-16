package net.novauniverse.spigot.version.shared.v1_16plus;

import org.bukkit.DyeColor;
import org.bukkit.Material;
import org.bukkit.inventory.ItemStack;

public class SharedBannerItemStackCreator {
	public static ItemStack getColoredBannerItemStack(DyeColor color) {
		return new ItemStack(SharedBannerItemStackCreator.getBannerMaterial(color), 1);
	}

	public static Material getBannerMaterial(DyeColor color) {
		switch (color) {
		case BLACK:
			return Material.BLACK_BANNER;

		case BLUE:
			return Material.BLUE_BANNER;

		case BROWN:
			return Material.BROWN_BANNER;

		case CYAN:
			return Material.CYAN_BANNER;

		case GRAY:
			return Material.GRAY_BANNER;

		case GREEN:
			return Material.GREEN_BANNER;

		case LIGHT_BLUE:
			return Material.LIGHT_BLUE_BANNER;

		case LIGHT_GRAY:
			return Material.LIGHT_GRAY_BANNER;

		case LIME:
			return Material.LIME_BANNER;

		case MAGENTA:
			return Material.MAGENTA_BANNER;

		case ORANGE:
			return Material.ORANGE_BANNER;

		case PINK:
			return Material.PINK_BANNER;

		case PURPLE:
			return Material.PURPLE_BANNER;

		case RED:
			return Material.RED_BANNER;

		case WHITE:
			return Material.WHITE_BANNER;

		case YELLOW:
			return Material.YELLOW_BANNER;

		default:
			return Material.WHITE_BANNER;
		}
	}
}