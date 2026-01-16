import { nanoid } from "nanoid";
import { now } from "#lib/dates";
import { logDebug } from "#lib/logs";
import { createValidator, taskInitSchema } from "#lib/json/schema";
import { setLocalStoreItem } from "#browser/local-storage";
import { getTasks } from "./get";
import type { ITask, ITaskInit } from "../types";

export async function createTask(init: ITaskInit): Promise<ITask> {
  const validate: Awaited<ReturnType<typeof createValidator<ITaskInit>>> =
    await createValidator(taskInitSchema.$id);
  validate(init);

  const [newTask] = await createTasks([init]);

  return newTask;
}

/**
 * @param inits
 * @returns Added tasks.
 */
async function createTasks(inits: ITaskInit[]): Promise<ITask[]> {
  logDebug(`Creating ${inits.length} tasks...`);

  const incomingTasks = inits.map(({ title, description, status }) => {
    const newTask: ITask = {
      id: nanoid(),
      title,
      description,
      status: status ?? "pending",
      created_at: now(),
      updated_at: now(),
    };

    return newTask;
  });

  const currentTasks = await getTasks();

  const newTasks = [...currentTasks, ...incomingTasks];
  setLocalStoreItem("todos", newTasks);

  logDebug(`Created ${inits.length} tasks.`);

  return incomingTasks;
}