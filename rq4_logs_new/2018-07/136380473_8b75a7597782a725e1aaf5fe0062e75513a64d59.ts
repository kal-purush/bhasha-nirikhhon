import { Component, OnInit } from '@angular/core';
import { ShowPostService } from './show-post.service';

@Component({
	selector: 'app-show-post',
	templateUrl: './show-post.component.html',
	styleUrls: ['./show-post.component.css'],
	providers: [ ShowPostService ]
})
export class ShowPostComponent implements OnInit {

	public posts : any [];

	constructor( private showPostService: ShowPostService){

	}

	ngOnInit(){
		this.getAllPost();
	}

	getAllPost(){
		this.showPostService.getAllPost().subscribe(result => {
			this.posts = result['data'];
		})
	}
}