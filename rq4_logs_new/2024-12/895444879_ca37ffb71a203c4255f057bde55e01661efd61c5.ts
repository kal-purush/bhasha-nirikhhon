import { TaskType } from "./TaskType";
import { SetTasksType } from "./SetTasksType";

export type TaskListType = {
  tasks: TaskType[];
  setTasks: SetTasksType;
};