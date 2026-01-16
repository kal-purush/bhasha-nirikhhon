import { nanoid } from "nanoid";
import dataExportSchema from "#schema/entities/data-export.schema.json";
import { now } from "#lib/dates";
import { createValidator } from "#lib/json/schema";
import { setLocalStoreItem } from "#browser/local-storage";
import { type ITodo, getTodos } from "#entities/todo";
import type { IDataExport } from "./types";

export async function createDataExport(): Promise<IDataExport> {
  const tasks = await getTodos();

  const dataExport: IDataExport = {
    version: 1,
    id: nanoid(),
    created_at: now(),
    data: {
      tasks,
    },
  };

  const validate: Awaited<ReturnType<typeof createValidator<IDataExport>>> =
    await createValidator<IDataExport>(dataExportSchema.$id);

  validate(dataExport);

  return dataExport;
}

export async function importDataExport(dataExport: unknown) {
  console.log(`Importing data export...`);

  console.debug(`Validating data export...`);

  const validate: Awaited<ReturnType<typeof createValidator<IDataExport>>> =
    await createValidator<IDataExport>(dataExportSchema.$id);

  validate(dataExport);

  console.debug(
    `Validated data export "${dataExport.id}" of version "${dataExport.version}".`,
  );

  console.log(
    `Importing ${dataExport.data.tasks.length} tasks of data export "${dataExport.id}"...`,
  );

  const updatedTasks = await importTasks(dataExport.data.tasks);
  setLocalStoreItem("todos", updatedTasks);

  console.log(
    `Imported tasks of data export "${dataExport.id}"...`,
  );

  console.log(`Imported data export "${dataExport.id}".`);
}

async function importTasks(incomingTasks: ITodo[]) {
  const currentTasks = await getTodos();
  const currentIDs = currentTasks.map(({ id }) => id);
  const newTasks = incomingTasks.filter(({ id }) => !currentIDs.includes(id));

  const updatedTasks = currentTasks.map<ITodo>((currentTask) => {
    const changedTask = incomingTasks.find(({ id }) => id === currentTask.id);

    if (
      !changedTask ||
      (currentTask.title === changedTask.title &&
        currentTask.description === changedTask.description)
    ) {
      return currentTask;
    } else {
      return changedTask;
    }
  });

  updatedTasks.push(...newTasks);

  return updatedTasks;
}