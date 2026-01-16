package com.vauff.maunz.core;

import org.pircbotx.PircBotX;
import org.pircbotx.hooks.ListenerAdapter;
import org.pircbotx.hooks.events.QuitEvent;

public class BreakInBad extends ListenerAdapter<PircBotX>
{
	public void onQuit(QuitEvent<PircBotX> event) throws Exception 
	{
		if (event.getUser().getNick().equals("BreakInBad"))
		{
			Main.esperBot.sendIRC().message("#BreakInBadStaff", "bl4ckscor3, Vakonof, Vauff, BreakInBad has just left IRC. If this was intentional, just ignore this message. Otherwise bring the server back up, I miss him :(");
		}
	}
}