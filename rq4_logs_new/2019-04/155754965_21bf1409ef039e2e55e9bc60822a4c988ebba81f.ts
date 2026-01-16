import { NgModule } from '@angular/core';
import { IonicPageModule } from 'ionic-angular';
import { QuizaddPage } from './quizadd';

@NgModule({
  declarations: [
    QuizaddPage,
  ],
  imports: [
    IonicPageModule.forChild(QuizaddPage),
  ],
})
export class QuizaddPageModule {}