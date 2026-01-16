import {Pipe} from 'angular2/angular2'
import {TaskModel} from './task-service'

@Pipe({
  name: 'tasksWithStatus',
  pure: false
})

export class TasksWithStatusFilter {

  transform(value, taskStatus) {
    return value.filter((task: TaskModel) => {
      return taskStatus == 'closed' ? task.isClosed() : !task.isClosed()
    })
  }

}