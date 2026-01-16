using System;
using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class TickSystem : MonoBehaviour {

	static TickSystem instance;
	static TickSystem Instance {
		get {
			if (!instance) {
				GameObject tickObject = new GameObject("TickSystem");
				instance = tickObject.AddComponent<TickSystem>();
			}

			return instance;
		}
	}

	private List<Action> onTickActions = new List<Action>();

	public static void Subscribe(Action subscriberAction) {
		Instance.onTickActions.Add(subscriberAction);
	}

	public static void Unsubscribe(Action subscriberAction) {
		Instance.onTickActions.Remove(subscriberAction);
	}
	
	private float timeToNextTick;
	private float tickFrequency = .2f;

	void Update () {
		timeToNextTick += Time.deltaTime;
		if (timeToNextTick >= tickFrequency) {
			timeToNextTick -= tickFrequency;
			ExecuteActions();
		}
	}

	void ExecuteActions() {
		onTickActions.ForEach(action => action());
	}
}