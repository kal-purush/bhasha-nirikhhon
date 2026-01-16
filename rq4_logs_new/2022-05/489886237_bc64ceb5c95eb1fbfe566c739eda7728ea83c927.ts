import { Component, OnInit, Input } from '@angular/core';
import { FormGroup } from '@angular/forms';
import { FormControl } from '@angular/forms';
import { Recipe } from 'src/app/shared/models/recipe';
import { CommentService } from 'src/app/shared/services/comment.service';
import { Comment } from '../../../../shared/models/comment';
import { AuthService } from 'src/app/shared/services/auth.service';

@Component({
	selector: 'app-comment',
	templateUrl: './comment.component.html',
	styleUrls: ['./comment.component.css']
})
export class CommentComponent implements OnInit {
	@Input() recipeInput?: Recipe;

	commentForm = new FormGroup({
		comment: new FormControl(""),
	});


	constructor(
		private commentService: CommentService,
		private authService: AuthService) { }

	ngOnInit(): void {
	}

	onSubmit() {
		let email: string = "";
		this.authService.getUser().subscribe(user => {
			const comment: Comment = {
				recipe: this.recipeInput?.name as string,
				email: user?.email as string,
				comment: this.commentForm.get("comment")?.value
			};
			this.commentService.create(comment);	
		});
	}



}