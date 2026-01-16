using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class TaskManager {

	private static List<Task> taskList = new List<Task>();

	public static Task GetNextTask() {
		Task next = taskList.Find(task => !task.taken);

		if (next != null) {
			next.taken = true;
		}

		return next;
	}

	public static void AddTask(Task task) {
		taskList.Add(task);
	}

	public static void FinishTask(Task task) {
		taskList.Remove(task);
	}

	public static void ReturnTask(Task task) {
		task.taken = false;
	}
}