package com.tjtofvesson.mcoverhaul.utils;

import net.minecraft.client.Minecraft;
import net.minecraft.client.gui.GuiNewChat;
import net.minecraft.util.text.ITextComponent;

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
}