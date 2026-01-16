package org.hydra.web.rest.response.json;

import java.util.HashMap;
import java.util.List;

import org.hydra.tasker.db.beans.Task;

public class MySubTasksJson {
	/**
	 * @return the subTasks
	 */
	public HashMap<String, SubTaskJson> getSubTasks() {
		return subTasks;
	}

	private HashMap<String, SubTaskJson> subTasks;

	public MySubTasksJson() {
		subTasks = new HashMap<String, SubTaskJson>();
	}

	public void addSubTasks(String taskId, List<Task> activeTasks) {
		SubTaskJson subTaskJson = new SubTaskJson();
		subTaskJson.addTasks(activeTasks);
		subTasks.put(taskId, subTaskJson);
	}
}