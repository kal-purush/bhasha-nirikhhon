import { Injectable } from '@angular/core';
import { WorkoutService } from './workout.service';
import { AlertController } from '@ionic/angular';

@Injectable({
  providedIn: 'root'
})
export class InputDialogService {

  constructor(private alertController: AlertController, public dataService: WorkoutService) { }

  async showPrompt(exercise?, index?) {
      const alert = await this.alertController.create({
        cssClass: 'my-custom-class',
        header: exercise ? 'Edit Exercise in Workout' : 'Add Exercise',
        message: exercise ? 'Edit exercise in the workout.' : 'Enter exercise to be added to the workouts.', 
        inputs: [
          {
            name: 'name',
            type: 'text',
            placeholder: 'Exercise Name',
            value: exercise ? exercise.name : null
          },
          {
            name: 'weight',
            type: 'number',
            placeholder: 'Weight Used (in LBs)',
            value: exercise ? exercise.weight : null
          },
          {
            name: 'sets',
            type: 'number',
            placeholder: 'Sets Completed',
            value: exercise ? exercise.weight : null
          },
          {
            name: 'reps',
            type: 'number',
            placeholder: 'Reps Completed',
            value: exercise ? exercise.weight : null
          },
        ],
        buttons: [
          {
            text: 'Cancel',
            role: 'cancel',
            cssClass: 'secondary',
            handler: exercise => {
              console.log('Confirm Cancel');
            }
          }, {
            text: 'Save',
            handler: exercise => {
              console.log('Exercise added', exercise);
              if (index !== undefined) {
                this.dataService.editExercise(exercise, index)
              }
              else {
                this.dataService.addExercise(exercise);
              }
            }
          }
        ]
      });

      await alert.present();
    }



}