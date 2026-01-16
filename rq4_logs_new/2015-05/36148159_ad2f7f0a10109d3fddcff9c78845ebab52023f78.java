package tk.mechtecs.organicmod;

import net.minecraft.creativetab.CreativeTabs;
import net.minecraft.init.Blocks;
import net.minecraft.item.Item;

public class CreativeTab {
	
	public static final CreativeTabs tabOrganics = new CreativeTabs(organicmod.MODID.toLowerCase() + ":general")
    {
        @Override
        public Item getTabIconItem()
        {
            return organicmod.tomato_crop;
        }
    };

}