import { Injectable } from '@angular/core';
import { Exercise } from './exercise.model';
import { Subject } from 'rxjs';

@Injectable({
  providedIn: 'root'
})
export class TrainingService {
  availableExercises: Exercise[] = [
    { id: 'crunches', name: 'Crunches', duration: 30, calories: 8 },
    { id: 'touch-toes', name: 'Touch Toes', duration: 180, calories: 15 },
    { id: 'side-lunges', name: 'Side Lunges', duration: 120, calories: 18 },
    { id: 'burpees', name: 'Burpees', duration: 60, calories: 8 }
  ];

  private runningExercise: Exercise;
  exerciseChanged = new Subject<Exercise>();
  private exercises: Exercise[] = [];

  constructor() { }

  getAvailableExercises(): Exercise[] {
    return this.availableExercises.slice();
  }

  startExercise(selectedId: string) {
    this.runningExercise = this.getExerciseById(selectedId);
    this.exerciseChanged.next(Object.assign({}, this.runningExercise));
  }

  private getExerciseById(selectedId: string): Exercise {

    const foundExercise = this.availableExercises
    .find(exercise => exercise.id === selectedId);
    return foundExercise;
  }

  getRunningExercise(): Exercise {
    return Object.assign({}, this.runningExercise);
  }

  completeExercise() {
    this.exercises.push(Object.assign({
      date: new Date(),
      state: 'completed'
    }, this.runningExercise));
    this.runningExercise = null;
    this.exerciseChanged.next(null);
  }

  cancelExercise(progress: number) {
    this.exercises.push(Object.assign({
      duration: this.runningExercise.duration * (progress / 100),
      calories: this.runningExercise.calories * (progress / 100),
      date: new Date(),
      state: 'cancelled'
    }, this.runningExercise));
    this.runningExercise = null;
    this.exerciseChanged.next(null);
  }
}