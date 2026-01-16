package net.clonecomputers.lab.darwin.keyboard;

import java.awt.event.KeyEvent;
import java.awt.event.KeyListener;
import java.util.Iterator;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;

import static java.awt.event.KeyEvent.*;

public class KeyHandler implements KeyListener, Iterable<KeyAction> {
	
	private Queue<KeyAction> actions = new ConcurrentLinkedQueue<KeyAction>();
	
	public KeyAction pollNextAction() {
		return actions.poll();
	}
	
	@Override
	public void keyPressed(KeyEvent e) {
		switch(e.getKeyCode()) {
			case VK_LEFT:
				actions.offer(KeyAction.PLAYER_MOVE_WEST);
				break;
			case VK_RIGHT:
				actions.offer(KeyAction.PLAYER_MOVE_EAST);
				break;
			case VK_UP:
				actions.offer(KeyAction.PLAYER_MOVE_NORTH);
				break;
			case VK_DOWN:
				actions.offer(KeyAction.PLAYER_MOVE_SOUTH);
				break;
		}
	}

	@Override
	public void keyReleased(KeyEvent e) {}

	@Override
	public void keyTyped(KeyEvent e) {}

	@Override
	public Iterator<KeyAction> iterator() {
		return new KeyActionIterator();
	}
	
	private class KeyActionIterator implements Iterator<KeyAction> {

		@Override
		public boolean hasNext() {
			return !actions.isEmpty();
		}

		@Override
		public KeyAction next() {
			return actions.poll();
		}

		@Override
		public void remove() {
			throw new UnsupportedOperationException();
		}
		
	}

}