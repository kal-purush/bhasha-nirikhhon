/// <reference path="../typings/angular2/angular2.d.ts" />

import {Component, View, bootstrap, NgFor} from "angular2/angular2";
import {Movie} from "../model/Movie";
import {MovieDbService} from "../service/MovieDbService";

@Component({
	selector: "movie-db",
	appInjector: [MovieDbService]
})
@View({
	templateUrl: "component/view/movie-db.html",
	directives: [NgFor]
})
class MovieDbComponent {
	
	private _movies: Array<Movie>;
	
	constructor(movieDbService: MovieDbService) {
		this._movies = movieDbService.getMovies();
	}
	
	get movies() {
		return this._movies;
	}
}

bootstrap(MovieDbComponent);