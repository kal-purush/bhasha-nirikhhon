package info.ccserver.homunculus;

import info.ccserver.homunculus.common.item.ItemFlask;
import net.minecraft.creativetab.CreativeTabs;
import net.minecraft.item.Item;
import net.minecraftforge.fml.common.registry.GameRegistry;
import net.minecraftforge.fml.relauncher.Side;
import net.minecraftforge.fml.relauncher.SideOnly;

public class HomunculusData {

	public static CreativeTabs tab;

	public static Item itemFlask;
	public static final String nameFlask = "flask";

	public static void registerPre() {
		tab = new TabHomunculus();

		itemFlask = new ItemFlask(nameFlask, tab);
		GameRegistry.registerItem(itemFlask, nameFlask);
	}

	public static void register() {

	}

	public static class TabHomunculus extends CreativeTabs {
		public static final String label = "Homunculus";

		TabHomunculus() {
			super(label);
		}

		@Override
		@SideOnly(Side.CLIENT)
		public Item getTabIconItem() {
			return itemFlask;
		}
	}

}