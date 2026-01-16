import {Component, Input, OnInit} from '@angular/core';
import {ReviewService} from '../../shared/service/review.service';

@Component({
  selector: 'app-chart-film',
  templateUrl: './chart-film.component.html',
  styleUrls: ['./chart-film.component.scss']
})
export class ChartFilmComponent implements OnInit {

  public likeWidth: string;
  public dislikeWidth: string;
  public countPositiveReviews = 0;
  public countNegativeReviews = 0;
  public isLoaded = false;
  private sumReviews = 0;
  @Input() idFilm: string;

  constructor(private reviews: ReviewService) {
  }

  ngOnInit() {
    this.reviews.getAllPositiveReviewsById(this.idFilm).subscribe((positiveReviews) => {
      this.countPositiveReviews = positiveReviews.length;
    });
    this.reviews.getAllNegativeReviewsById(this.idFilm).subscribe((negativeReviews) => {
      this.countNegativeReviews = negativeReviews.length;
      this.setDiagram();
      this.isLoaded = true;
    });
  }

  private setDiagram(): void {
    this.sumReviews = this.countPositiveReviews + this.countNegativeReviews;
    this.likeWidth = Math.round((this.countPositiveReviews / this.sumReviews * 100))   + '%';
    this.dislikeWidth = Math.round((this.countNegativeReviews / this.sumReviews * 100))  + '%';
    console.log(this.likeWidth, this.dislikeWidth);
  }

}