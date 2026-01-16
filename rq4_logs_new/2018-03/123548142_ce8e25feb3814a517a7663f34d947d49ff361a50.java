package com.cookiehook.liquidenchanting.proxy;

import com.cookiehook.liquidenchanting.init.ModItems;

public class ClientProxy extends CommonProxy {

	/**
	 * Registers the rendering, only on the client. If this were called on the server, the server would crash.
	 */
	@Override
	public void registerRenders() {
		ModItems.registerRenders();
	}
}