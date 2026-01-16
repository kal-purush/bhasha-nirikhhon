package xsked.net;

import sk.net.SKConnection;
import sk.net.SKPacket;
import sk.net.SKPacketListener;
import time.api.gamestate.GameStateManager;
import xsked.states.StateMenuHost;

public class GameplayListener implements SKPacketListener {
	
	@Override
	public void connected(SKConnection connection) {
		if(NetworkManager.isServer()) {
			((StateMenuHost) GameStateManager.getGameState("Host Menu")).setDisplay("");;
		}
		NetworkManager.setConnected(true);
	}
	
	@Override
	public void disconnected(SKConnection connection, boolean local, String msg) {
		
	}
	
	@Override
	public void received(SKConnection connection, SKPacket packet) {
		
	}
}