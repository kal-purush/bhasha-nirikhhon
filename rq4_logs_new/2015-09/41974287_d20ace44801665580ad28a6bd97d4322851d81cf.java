package com.cadyyan.lockAndKey;

import net.minecraft.block.Block;

import java.util.Set;

public class Settings
{
	public static class Locks
	{
		public static boolean whitelistEnabled;
		public static Set<Block> blockWhitelist;

		public static boolean blacklistEnabled;
		public static Set<Block> blockBlacklist;

		private Locks()
		{
		}
	}

	private Settings()
	{
	}
}