package com.tjtofvesson.mcoverhaul.api;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import net.minecraft.client.Minecraft;
import net.minecraft.client.gui.GuiNewChat;
import net.minecraft.util.text.ITextComponent;
import net.minecraft.util.text.Style;

/**
 * Class stolen from TechReborn, which was stolen from SteamAgeRevolution, which was stolen from BloodMagic, which was stolen from EnderCore, which stole the
 * idea from ExtraUtilities, who stole it from vanilla.
 *
 * Original class link:
 * https://github.com/SleepyTrousers/EnderCore/blob/master/src/main/java/com/enderio/core/common/util/ChatUtil.java
 */

public class ChatUtils
{
	private static final int DELETION_ID = 551; //MAKE THIS UNIQUE PER MOD THAT USES THIS

	public static void sendNoSpamMessages(int messageID, ITextComponent message)
	{
		int deleteID = DELETION_ID+messageID;
		GuiNewChat chat = Minecraft.getMinecraft().ingameGUI.getChatGUI();
		chat.printChatMessageWithOptionalDeletion(message, deleteID);
	}
	
	public static ITextComponent wrapMessage(final String message){
		return new ITextComponent(){
			
			String s = message;
			
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
				s += text;
				return this;
			}

			@Override
			public ITextComponent appendSibling(ITextComponent component) {
				this.appendText(component.getFormattedText());
				return this;
			}

			@Override
			public String getUnformattedComponentText() {
				return s;
			}

			@Override
			public String getUnformattedText() {
				return s;
			}

			@Override
			public String getFormattedText() {
				return s;
			}

			@Override
			public List<ITextComponent> getSiblings() {
				return new ArrayList<ITextComponent>();
			}

			@Override
			public ITextComponent createCopy() {
				return this;
			}};
	}
}