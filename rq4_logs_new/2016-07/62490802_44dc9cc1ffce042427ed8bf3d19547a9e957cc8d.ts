import {autoinject}  from 'aurelia-framework';
import {NPM, FS}     from 'monterey-pal';
import {TaskManager} from '../../shared/task-manager';

@autoinject()
export class Common {

  constructor(private taskManager: TaskManager) {
  }

  installNPMDependencies(project) {
    let task = {
      title: `npm install of '${project.name}'`,
      estimation: 'This could take minutes to complete',
      logs: [],
      promise: null
    };

    let promise = NPM.install([], {
      npmOptions: {
        workingDirectory: FS.getFolderPath(project.packageJSONPath)
      },
      logCallback: (message) => {
        if (message.level === 'custom' || message.level === 'warning' || message.level === 'error') {
          this.taskManager.addTaskLog(task, message.message);
        }
      }
    });

    task.promise = promise;

    this.taskManager.addTask(task);
  }
}