/// <reference path="../typings/angular2/angular2.d.ts" />

import {
	Component,
	View
} from 'angular2/angular2';

@Component({
	selector: 'reading-list-form'
})

@View({
	template: `<form>
		<div class="form-group">
			<div><label for="title">Title:</label></div>
			<div><input type="text" name="title" class="form-control" #newtitle /></div>
		</div>
		<div class="form-group">
			<div><label for="link">Link:</label></div>
			<div><input type="text" name="link" class="form-control" #newlink /></div>
		</div>
		<button class="btn btn-primary" (click)="addArticle(newtitle, newlink)" >Submit Article</button>
	</form>`
})

export class ReadingListForm {
	addArticle(title, link) {
		console.log(title.value, link.value);
		return false;
	}
}