package xsked.net;

import java.util.PriorityQueue;

import sk.net.SKPacket;
import time.api.gamestate.GameStateManager;

public class EventQueue {
	
	private static final PriorityQueue<SKPacket> queue = new PriorityQueue<>();
	
	public static final synchronized void process() {
		while(!queue.isEmpty()) {
			SKPacket packet = queue.poll();
			
			if(packet instanceof PacketState) {
				GameStateManager.enterState(((PacketState) packet).STATE);
			}
		}
	}
	
	public static final synchronized void queue(SKPacket packet) {
		queue.add(packet);
	}
}