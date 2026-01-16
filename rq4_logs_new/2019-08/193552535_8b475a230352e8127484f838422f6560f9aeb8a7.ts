import { Component, EventEmitter, Input, Output } from '@angular/core';
import { FormControl, FormGroup } from '@angular/forms';

@Component({
	selector: 'app-edit-recipe-step',
	templateUrl: './edit-recipe-step.component.html',
	styleUrls: ['./edit-recipe-step.component.scss']
})
export class EditRecipeStepComponent {
	@Input() recipeStep;
	@Input() recipeStepIndex;
	@Output() deleteStepKey = new EventEmitter<number>();
	editedStep = new FormGroup({
		stepName: new FormControl('')
	});
	isStepEdited = false;

	deleteStep(key: number) {
		this.deleteStepKey.emit(key);
	}

	updateStepName() {
		this.recipeStep.value.name = this.editedStep.value.stepName;
		this.isStepEdited = false;
	}
}