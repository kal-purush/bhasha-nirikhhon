import { TaskEntity } from "src/entities/task.entity";

export interface IGetTask extends Partial<TaskEntity> {
  username: string
}