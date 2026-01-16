import { Injectable } from '@angular/core';
import { InMemoryDbService } from 'angular-in-memory-web-api';
import { Task } from '../models/task';

@Injectable({
  providedIn: 'root'
})
export class InMemoryDataService implements InMemoryDbService {
  createDb() {
    const tasks: Task[] = [
      { id: 1, title: 'Task 1', description: 'Description 1', completed: false },
      { id: 2, title: 'Task 2', description: 'Description 2', completed: true }
    ];
    return { tasks }; // "tasks" — это имя коллекции для API
  }

  // Генерация ID для новых задач
  genId(tasks: Task[]): number {
    return tasks.length > 0 ? Math.max(...tasks.map(task => task.id)) + 1 : 1;
  }
}