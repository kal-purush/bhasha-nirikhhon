package net.zeeraa.novacore.bungeecord.delayedrunner;

import java.util.concurrent.TimeUnit;

import net.md_5.bungee.api.ProxyServer;
import net.zeeraa.novacore.bungeecord.NovaCore;
import net.zeeraa.novacore.commons.utils.DelayedRunnerImplementation;

public class DelayedRunnerImplementationBungee implements DelayedRunnerImplementation{
	@Override
	public void runDelayed(Runnable runnable, long ticks) {
		ProxyServer.getInstance().getScheduler().schedule(NovaCore.getInstance(), runnable, ticks * 50, TimeUnit.MILLISECONDS);
	}
}