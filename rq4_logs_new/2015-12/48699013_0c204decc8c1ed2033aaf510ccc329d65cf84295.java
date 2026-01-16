package xsked.net;

import sk.net.SKPacket;
import time.api.debug.Debug;
import time.api.input.InputManager;
import time.api.math.Vector2f;
import xsked.level.Level;

public class LevelSender {
	
	public static final float UPDATES_PER_SECOND = 10f;
	
	private static Level level;
	
	private static float px, py;
	
	private static float deltaStack = 0;
	
	public static final void updateWizard(float dt) {
		deltaStack += dt;
		if(deltaStack > 1f / UPDATES_PER_SECOND) {
			
			
			deltaStack = 0;
		}
	}
	
	public static final void updateApprentice(float dt) {
		
		//Send player position
		if(InputManager.isDown("p_left") || InputManager.isDown("p_right")
				|| InputManager.isDown("p_down") || InputManager.isDown("p_jump")) {
			
		}
		
		deltaStack += dt;
		if(deltaStack > 1f / UPDATES_PER_SECOND) {
			
			Vector2f vel = level.getPlayer().getBody().getVel();
			sendPacket(new PacketPosition(level.getPlayer().getX(), level.getPlayer().getY(), 0, "player", vel.getX(), vel.getY()));
			
			deltaStack = 0;
		}
		
		
	}
	
	private static final void sendPacket(SKPacket packet) {
		if(NetworkManager.isClient())
			NetworkManager.getClient().send(packet);
		else if(NetworkManager.isServer())
			NetworkManager.getServer().send(0, packet);
	}
	
	public static final void setLevel(Level level) {
		LevelSender.level = level;
		
		extractValues();
	}
	
	private static final void extractValues() {
		px = level.getPlayer().getX();
		py = level.getPlayer().getY();
	}
}