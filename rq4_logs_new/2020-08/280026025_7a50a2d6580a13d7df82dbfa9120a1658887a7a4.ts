import { NgModule } from '@angular/core';
import { CommonModule } from '@angular/common';
import { FormsModule } from '@angular/forms';

import { IonicModule } from '@ionic/angular';

import { WorkoutExercisesPageRoutingModule } from './workout-exercises-routing.module';

import { WorkoutExercisesPage } from './workout-exercises.page';

@NgModule({
  imports: [
    CommonModule,
    FormsModule,
    IonicModule,
    WorkoutExercisesPageRoutingModule
  ],
  declarations: [WorkoutExercisesPage]
})
export class WorkoutExercisesPageModule {}