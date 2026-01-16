/**
 * 
 */
package com.tjtofvesson.mcoverhaul;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import com.tjtofvesson.mcoverhaul.proxy.CommonProxy;

import net.minecraft.util.text.ITextComponent;
import net.minecraft.util.text.Style;
import net.minecraftforge.common.MinecraftForge;
import net.minecraftforge.fml.common.Mod;
import net.minecraftforge.fml.common.Mod.EventHandler;
import net.minecraftforge.fml.common.Mod.Instance;
import net.minecraftforge.fml.common.SidedProxy;
import net.minecraftforge.fml.common.event.FMLInitializationEvent;
import net.minecraftforge.fml.common.event.FMLPostInitializationEvent;
import net.minecraftforge.fml.common.event.FMLPreInitializationEvent;
import net.minecraftforge.fml.common.eventhandler.SubscribeEvent;
import net.minecraftforge.fml.common.gameevent.PlayerEvent;

/**
 * @author TjKenMate, Blynd3
 *
 */

@Mod(modid = Reference.MOD_ID, name = Reference.NAME, version = Reference.VERSION, acceptedMinecraftVersions = Reference.MC_VERS)

public class MCOverhaul {

	@Instance
	public static MCOverhaul instance;
	
	@SidedProxy(clientSide = Reference.CLIENT_PROXY_CLASS, serverSide = Reference.SERVER_PROXY_CLASS)
	public static CommonProxy proxy;
	
	@EventHandler
	public void preInit(FMLPreInitializationEvent event)
	{
		System.out.println("HAHAH PRE");
	}
	
	@EventHandler
	public void init(FMLInitializationEvent event)
	{
		System.out.println("HAHAH PRE");
        MinecraftForge.EVENT_BUS.register(this);
	}
	
	@EventHandler
	public void postInit(FMLPostInitializationEvent event)
	{
		System.out.println("HAHAH PRE");
	}
	
    @SubscribeEvent
    public void onPlayerLogin(PlayerEvent.PlayerLoggedInEvent event) {
    	System.out.println("Ayylmao: "+event.player.getName());
    	event.player.addChatMessage(new ITextComponent(){

			@Override
			public Iterator<ITextComponent> iterator() {
				List<ITextComponent> l = new ArrayList<ITextComponent>();
				l.add(this);
				return l.iterator();
			}

			@Override
			public ITextComponent setStyle(Style style) {
				return this;
			}

			@Override
			public Style getStyle() {
				return new Style();
			}

			@Override
			public ITextComponent appendText(String text) {
				return this;
			}

			@Override
			public ITextComponent appendSibling(ITextComponent component) {
				return this;
			}

			@Override
			public String getUnformattedComponentText() {
				return "Dank Memes";
			}

			@Override
			public String getUnformattedText() {
				return "Dank Memes";
			}

			@Override
			public String getFormattedText() {
				return "Dank Memes";
			}

			@Override
			public List<ITextComponent> getSiblings() {
				return new ArrayList<ITextComponent>();
			}

			@Override
			public ITextComponent createCopy() {
				return this;
			}
		});
    }
}