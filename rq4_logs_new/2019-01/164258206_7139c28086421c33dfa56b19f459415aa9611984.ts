import { Component, OnInit } from '@angular/core';
import { ActivatedRoute } from '@angular/router';
import { MovieService } from 'src/app/providers/movie.service';
import { Movie } from 'src/app/interfaces/movie.interface';
import { DocumentSnapshot } from '@angular/fire/firestore';
import { ReviewService } from 'src/app/providers/review.service';
import { Review } from 'src/app/interfaces/review.interface';
import { faPlus } from '@fortawesome/free-solid-svg-icons';

@Component({
  selector: 'app-movie-detail',
  templateUrl: './movie-detail.component.html',
  styles: []
})
export class MovieDetailComponent implements OnInit {

  faPlus = faPlus;

  movie: Movie;
  reviews: Review[];

  constructor(private activatedRoute:ActivatedRoute,
              private movieService: MovieService,
              private reviewService: ReviewService) {
    this.activatedRoute.params.subscribe(params => {
      this.getMovie(params['id']);
      this.getReviews(params['id'])
    });
  }

  getMovie = (id: string) => {
    this.movieService.getMovie(id).subscribe((movieDocument: DocumentSnapshot<Movie>) => {
      this.movie = movieDocument.data();
    });
  }

  getReviews = (movieId: string) => {
    this.reviewService.getReviewsByMovie(movieId).subscribe((reviews: Review[]) => {
      this.reviews = reviews;
    })
  }

  ngOnInit() {
  }

}