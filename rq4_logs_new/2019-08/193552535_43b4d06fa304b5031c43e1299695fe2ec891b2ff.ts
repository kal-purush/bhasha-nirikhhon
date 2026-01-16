import { Component, EventEmitter, Input, Output } from '@angular/core';
import { RecipesService } from '../../recipes.service';
import { Router } from '@angular/router';

@Component({
	selector: 'app-delete-recipe-modal',
	templateUrl: './delete-recipe-modal.component.html',
	styleUrls: ['./delete-recipe-modal.component.scss']
})
export class DeleteRecipeModalComponent {
	@Input() isModalActive = false;
	@Input() activeRecipeId: string;
	@Output() modalClosed = new EventEmitter<boolean>();

	constructor(private recipesService: RecipesService, private router: Router) {}

	closeModal() {
		this.isModalActive = false;
		this.modalClosed.emit(true);
	}

	deleteRecipe() {
		this.recipesService.deleteRecipe(this.activeRecipeId);
		this.closeModal();
		this.router.navigate(['/recipes']);
	}
}