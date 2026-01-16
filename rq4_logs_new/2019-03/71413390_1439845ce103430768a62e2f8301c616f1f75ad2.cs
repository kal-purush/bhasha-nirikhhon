using UnityEngine;
using System.Collections;
using System.Collections.Generic;

enum State {
	Idle,
	Working
}

public class WorkerAI : MonoBehaviour {

	private Task currentTask = null;
	private State state = State.Idle;

	[SerializeField]
	private float taskCheckFrequency = .2f;
	private float waitingTimer;

	void Update() {
		switch(state) {
			case State.Idle: GetNextTask();
				break;

			case State.Working: UpdateTask();
				break;
		}
	}

	void GetNextTask() {
		waitingTimer -= Time.deltaTime;
		if (waitingTimer > 0) return;
		waitingTimer = taskCheckFrequency;

		currentTask = TaskManager.GetNextTask();
		if (currentTask)
			state = State.Working;
	}

	void UpdateTask() {
		bool taskOk = currentTask.Update(gameObject);

		if (!taskOk) {
			Abort();
			return;
		}

		if (currentTask.IsDone()) {
			TaskManager.FinishTask(currentTask);
			currentTask = null;
			state = State.Idle;
		}
	}

	private void Abort() {
		TaskManager.ReturnTask(currentTask);
		currentTask = null;
		state = State.Idle;
	}
}