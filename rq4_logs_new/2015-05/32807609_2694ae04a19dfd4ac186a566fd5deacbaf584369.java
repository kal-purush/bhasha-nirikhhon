package net.clonecomputers.lab.darwin.keyboard;

public interface KeyMap {
	/**
	 * Get the action that is bound to a key code.
	 * @param keyCode
	 * @return The action that is bound to that key code,
	 * or null if no action is bound to that key code.
	 */
	public KeyAction getAction(int keyCode);
	
	/**
	 * Bind an action to a key code.
	 * @param keyCode
	 * @param action
	 * @return The action that was previously bound to that key code,
	 * or null if no action was previously bound.
	 */
	public KeyAction bindAction(int keyCode, KeyAction action);
	
	/**
	 * Unbind an action from a key code.
	 * @param keyCode
	 * @return The action that was previously bound to that key code,
	 * or null if no action was previously bound.
	 */
	public KeyAction unbindAction(int keyCode);
}