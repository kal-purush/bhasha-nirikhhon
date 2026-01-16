// export type Task<Res> = () => Promise<Res>
type TaskStatus = 'IDLE' | 'SUCCESS' | 'ERROR' | 'PENDING' | 'STOP'
class Task {
  status: TaskStatus = 'IDLE'
  promise: () => Promise<any>

  constructor(promise: () => Promise<any>) {
    this.promise = promise
  }
}

export class TaskPool<TaskRes = any> {
  taskList: any[] = []
  max = 6
  min = 2

  constructor() {
  }

  add(item: any) {
    this.taskList.push(new Task(item))
  }

  clear() {
    this.taskList = []
  }

  start() {
    const { min, max, taskList } = this

    while (min <= taskList.length && taskList.length < max) {

    }
  }
}