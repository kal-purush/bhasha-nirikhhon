using UnityEngine;
using System.Collections;

public enum StateIdentifier {
	STATE1,
	STATE2,
	STATE3
}

public interface BaseState {
	void OnStateEnter();
	void OnStateExit();
	void OnUpdate();
}

public static class StateManager {
	public static BaseState currentState;

	public static void SwitchState(BaseState newState) {
		currentState.OnStateExit();
		currentState = newState;
		currentState.OnStateEnter();
	}

	public static void Update() {
		if(currentState != null)
			currentState.OnUpdate();
	}

	public static BaseState GetState() {
		return currentState;
	}
}