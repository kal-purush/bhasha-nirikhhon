using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class Action {

	private ICommand currentCommand = null;
	private Queue<ICommandPrefab> commandQueue = new Queue<ICommandPrefab>();
	private GameObject actingObject;
	private bool finished = false;

	public Action(GameObject actingObject, ActionEnum action, ICommandArgs args) {

		this.actingObject = actingObject;

		switch (action) {
			case ActionEnum.MOVE:
				CommandPrefab<MoveCommand> newCommand = new CommandPrefab<MoveCommand>(actingObject, args);
				commandQueue.Enqueue((ICommandPrefab)newCommand);
				break;

			case ActionEnum.BUILD:
				BuildCommandArgs buildArgs = args as BuildCommandArgs;
				if (buildArgs == null)
					throw new UnityException("Wrong type of args");

				Vector3 temp = buildArgs.position;
				temp.x = temp.x - 1;
				MoveCommandArgs moveArgs = new MoveCommandArgs(buildArgs.position, buildArgs.moveTime);

				CommandPrefab<MoveCommand> moveCommand = new CommandPrefab<MoveCommand>(actingObject, moveArgs);
				commandQueue.Enqueue((ICommandPrefab)moveCommand);

				CommandPrefab<BuildCommand> buildCommand = new CommandPrefab<BuildCommand>(actingObject, buildArgs);
				commandQueue.Enqueue((ICommandPrefab)buildCommand);
				break;
		}
	}

	public void Update() {
		if (currentCommand == null && commandQueue.Count > 0) {
			ICommandPrefab commandPrefab = commandQueue.Dequeue();
			actingObject.AddComponent(commandPrefab.GetComponentType());
			currentCommand = commandPrefab.Execute();
		}

		if (currentCommand != null && currentCommand.isFinished()) {
			CleanCommand();
		}

		if (currentCommand == null && commandQueue.Count == 0) {
			finished = true;
		}
	}

	public bool IsFinished() {
		return finished;
	}

	private void CleanCommand() {
		UnityEngine.Object.Destroy((MonoBehaviour)currentCommand);
		currentCommand = null;
	}

	public void CleanAllCommands()
	{
		commandQueue.Clear();
		if (currentCommand != null) {
			currentCommand.Stop();
		}
	}
}