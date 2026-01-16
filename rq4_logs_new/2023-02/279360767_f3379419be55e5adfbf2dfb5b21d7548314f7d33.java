package net.zeeraa.novacore.spigot.utils;

import java.util.List;

import org.bukkit.DyeColor;
import org.bukkit.block.banner.Pattern;
import org.bukkit.block.banner.PatternType;
import org.bukkit.inventory.ItemStack;
import org.bukkit.inventory.meta.BannerMeta;
import net.zeeraa.novacore.spigot.abstraction.VersionIndependentUtils;

public class BannerBuilder {
	private ItemStack item;
	private BannerMeta meta;

	public BannerBuilder() {
		this(DyeColor.WHITE);
	}

	public BannerBuilder(DyeColor baseColor) {
		item = VersionIndependentUtils.get().getColoredBannerItemStack(baseColor);
		meta = (BannerMeta) item.getItemMeta();
	}

	public BannerBuilder(ItemStack bannerItemStack) {
		if (!bannerItemStack.getType().name().contains("BANNER")) {
			throw new IllegalArgumentException("Provided ItemStack needs to be of type BANNER, " + bannerItemStack.getType().name() + " detected. If this is actually a banner please open an issue on our github repo");
		}
		item = bannerItemStack;
		meta = (BannerMeta) bannerItemStack.getItemMeta();
	}

	public BannerBuilder addPattern(Pattern pattern) {
		meta.addPattern(pattern);
		return this;
	}

	public BannerBuilder addPattern(DyeColor color, PatternType type) {
		meta.addPattern(new Pattern(color, type));
		return this;
	}

	public BannerBuilder setPattern(int index, Pattern pattern) {
		meta.setPattern(index, pattern);
		return this;
	}

	public BannerBuilder setPattern(int index, DyeColor color, PatternType type) {
		meta.setPattern(index, new Pattern(color, type));
		return this;
	}

	public BannerBuilder removePattern(int index) {
		meta.removePattern(index);
		return this;
	}

	public BannerBuilder setPatterns(List<Pattern> patterns) {
		meta.setPatterns(patterns);
		return this;
	}

	public int numberOfPatterns() {
		return meta.numberOfPatterns();
	}

	public List<Pattern> getPatterns() {
		return meta.getPatterns();
	}

	public ItemStack build() {
		return this.build(false);
	}

	public ItemStack build(boolean clone) {
		item.setItemMeta(meta);
		return clone ? item.clone() : item;
	}

	public ItemBuilder toItemBuilder() {
		return this.toItemBuilder(false);
	}

	public ItemBuilder toItemBuilder(boolean clone) {
		return new ItemBuilder(this.build(clone));
	}
}