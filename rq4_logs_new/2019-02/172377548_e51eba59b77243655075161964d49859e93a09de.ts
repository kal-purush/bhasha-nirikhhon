import { Component } from '@angular/core';
import { Task } from './models/task/task';
import { Group } from './models/group/group';

@Component({
  selector: 'app-root',
  templateUrl: './app.component.html',
  styleUrls: ['./app.component.css']
})
export class AppComponent {

  /**
   * List of tasks
   */
  tasks: Array<Task> = [];

  /**
   * List of groups
   */
  groups: Array<Group> = [];

  /**
   * Task to add from input
   */
  taskToAdd: string;

  /**
   * Group to add from input
   */
  groupToAdd: string;

  /**
   * Date to add from input
   */
  taskDateToAdd: Date;

  constructor() {
    this.loadGroups();
  }

  /**
   * Load groups from localStorage.
   */
  loadGroups() {
    // Check localStorage for tasks
    if (localStorage.getItem('groups')) {
      // Tasks are saved in localStorage, loop and add to tasks
      for (const item of JSON.parse(localStorage.getItem('groups'))) {
        // Setup group to add to groups later
        const group: Group = new Group(item.name, item.tasks);
        // Add all task objects to Task class
        for (const task of item.tasks) {
          group.tasks.push(new Task(task.name, task.done, task.date, task.pin));
        }
        // Add group to groups
        this.groups.push(group);
      }
    }
  }

  /**
   * Save groups to localStorage
   */
  saveGroup() {
    localStorage.setItem('groups', JSON.stringify(this.groups));
  }

  /**
   * Add a task to task list and save to localStorage
   */
  addTask(group: Group) {
    group.tasks.unshift(new Task(this.taskToAdd, false, new Date(this.taskDateToAdd), false));
    this.saveGroup();
    this.taskToAdd = '';
  }

  /**
   * Add a group to group list and save to localStorage
   */
  
  addGroup() {
    if(!this.groupToAdd){
      return;
    }
    else{
     this.groups.unshift(new Group(this.groupToAdd, []));
     this.saveGroup();
     this.groupToAdd = '';
    }
  }
  /**
   * Delete a task and save to localStorage
   * @todo Delete a task from group
   */
  deleteTask(group: Group, task: Task) {
    // Show are-you-sure message
    if (!confirm('Are you sure you want to delete?')) {
      return;
    }
    // Find index of task
    const index: number = group.tasks.indexOf(task);
    // Delete task at index
    group.tasks.splice(index, 1);
    // Save to localStorage
    this.saveGroup();
  }

  pinTask(task: Task) {
    task.pin = !task.pin;
    this.saveGroup();
  }

  // archive
  archiveTask(task: Task){
    task.pin = !task.pin;
    this.saveGroup();
  }
}
