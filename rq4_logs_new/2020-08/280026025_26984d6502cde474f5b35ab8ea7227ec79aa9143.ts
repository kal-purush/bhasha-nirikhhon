import { Injectable } from '@angular/core';
import { AlertController } from '@ionic/angular';
import { WorkoutServiceService } from './workout-service.service';

@Injectable({
  providedIn: 'root'
})
export class InputDialogServiceService {

  constructor(private alertController: AlertController, public dataService: WorkoutServiceService) { }

async showPrompt(exercise?, index?) {
    const alert = await this.alertController.create({
      cssClass: 'my-custom-class',
      header: exercise ? 'Edit Exercise in Workout' : 'Add Exercise',
      message: exercise? "Edit exercise in the workout." : "Enter exercise to be added to the workoust.", 
      inputs: [
        {
          name: 'name',
          type: 'text',
          placeholder: 'name',
          value: exercise ? exercise.name : null
        },
        {
          name: 'weight',
          type: 'number',
          placeholder: 'weight',
          value: exercise ? exercise.weight : null
        }
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