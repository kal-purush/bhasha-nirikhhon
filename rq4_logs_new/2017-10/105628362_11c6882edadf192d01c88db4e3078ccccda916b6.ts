import { NgModule } from '@angular/core';
import { SeqCodeChallengeComponent } from './seq-code-challenge/seq-code-challenge';
import { SeqMultipleChoiceComponent } from './seq-multiple-choice/seq-multiple-choice';
import { IonicModule } from "ionic-angular";
import { MyApp } from "../app/app.component";
import { SeqOpenEndedComponent } from './seq-open-ended/seq-open-ended';

@NgModule({
	declarations: [SeqCodeChallengeComponent,
    SeqMultipleChoiceComponent,
    SeqOpenEndedComponent],
	imports: [IonicModule.forRoot(SeqMultipleChoiceComponent)],
	exports: [SeqCodeChallengeComponent,
    SeqMultipleChoiceComponent,
    SeqOpenEndedComponent]
})
export class ComponentsModule {}