package xsked;

import time.api.Game;
import time.api.gamestate.GameStateManager;
import xsked.states.*;

public class Main {
	
	public static final String NAME = "Help I'm Stuck In a Wizzard-Tower";
	public static final String NAME_ABBREVIATION = "HIZIW";
	public static final int WIDTH = 1280;
	public static final int HEIGHT = 720;
	
	public static final void main(String[] args) {
		
		Game game = new Game();
		
		//Menu states
		GameStateManager.registerState(new StateMenuMain(game));
		GameStateManager.registerState(new StateMenuPlay(game));
		GameStateManager.registerState(new StateMenuHost(game));
		GameStateManager.registerState(new StateMenuConnect(game));
		
		//Gameplay states
		GameStateManager.registerState(new StateWizard());
		GameStateManager.registerState(new StateApprentice());
		
		game.run(NAME, WIDTH, HEIGHT);
	}
}