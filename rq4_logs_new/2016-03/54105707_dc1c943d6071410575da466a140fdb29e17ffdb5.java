package com.hhi.bigdata.platform.push.server;

import asia.stampy.common.StampyLibrary;
import asia.stampy.common.gateway.AbstractStampyMessageGateway;
import com.hhi.bigdata.platform.push.server.listener.TestServerMessageListener;
import com.hhi.bigdata.platform.push.server.netty.NettyInitializer;

/**
 * Receives message from a test client and sends receipts if requested.
 */
@StampyLibrary(libraryName = "stampy-examples")
public class PushServer {
	private AbstractStampyMessageGateway gateway;

	/**
	 * Inits the.
	 *
	 * @throws Exception
	 *           the exception
	 */
	public void init() throws Exception {
		setGateway(NettyInitializer.initialize());

		gateway.addMessageListener(new TestServerMessageListener(), 1);

		gateway.connect();
		System.out.println("Stampy server started");
	}

	/**
	 * Gets the gateway.
	 *
	 * @return the gateway
	 */
	public AbstractStampyMessageGateway getGateway() {
		return gateway;
	}

	/**
	 * Sets the gateway.
	 *
	 * @param gateway
	 *          the new gateway
	 */
	public void setGateway(AbstractStampyMessageGateway gateway) {
		this.gateway = gateway;
	}
}