package controller;

import model.Model;
import controller.Game.ControllerGame;
import controller.Lobby.ControllerLobby;

public class Controller {

	private ControllerLobby controllerLobby;
	private ControllerGame controllerGame;

	public Controller(Model model) {
		controllerLobby = new ControllerLobby(model);
		controllerGame = new ControllerGame(model);
	}

	public ControllerLobby getControllerLobby() {
		return controllerLobby;
	}

	public ControllerGame getControllerGame() {
		return controllerGame;
	}
}