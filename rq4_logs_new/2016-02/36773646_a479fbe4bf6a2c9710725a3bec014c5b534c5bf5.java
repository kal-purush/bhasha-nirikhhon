package io.darkcraft.darkutils.mod.commands;

import io.darkcraft.darkcore.mod.abstracts.AbstractCommandNew;
import io.darkcraft.darkutils.mod.handlers.Unifier;

import java.util.List;

import net.minecraft.command.ICommandSender;
import net.minecraft.entity.player.EntityPlayer;

public class UnifierCommand extends AbstractCommandNew
{

	@Override
	public String getCommandName()
	{
		return "unifier";
	}

	@Override
	public void getAliases(List<String> list)
	{
	}

	@Override
	public boolean canCommandSenderUseCommand(ICommandSender comSen)
	{
		if(comSen instanceof EntityPlayer)
		{
			return true;
		}
		return false;
	}

	@Override
	public boolean process(ICommandSender sen, List<String> strList)
	{
		if(sen instanceof EntityPlayer)
		{
			EntityPlayer pl = (EntityPlayer) sen;
			if(strList.size() == 0)
				sendString(sen,"Expected args: toggle");
			else
			{
				if(strList.get(0).equals("toggle"))
				{
					Unifier.togglePlayer(pl);
					return true;
				}
			}
		}
		return false;
	}

}