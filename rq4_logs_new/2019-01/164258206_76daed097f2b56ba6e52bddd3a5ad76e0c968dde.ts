import { Component, OnInit } from '@angular/core';
import { QuerySnapshot, QueryDocumentSnapshot, DocumentReference, DocumentSnapshot } from '@angular/fire/firestore';
import { TagService } from '../../providers/tag.service';
import { MovieService } from '../../providers/movie.service';
import { Tag } from 'src/app/interfaces/tag.interface';
import { Movie } from 'src/app/interfaces/movie.interface';

@Component({
  selector: 'app-top-voted',
  templateUrl: './top-voted.component.html',
  styles: []
})
export class TopVotedComponent implements OnInit {

  tags: Tag[];
  movies: Movie[];

  constructor(private tagService: TagService,
              private movieService: MovieService) {
    this.tagService.getTags().subscribe((data: QuerySnapshot<Tag>) => {
      this.tags = [];
      data.forEach((tag: QueryDocumentSnapshot<Tag>) => {
        this.tags.push(tag.data());
      });
    });
    this.movieService.getTopVoted().subscribe((data: QuerySnapshot<Movie>) => {
      this.movies = [];
      data.forEach((movie: QueryDocumentSnapshot<Movie>) => {
        this.movies.push(movie.data());
      })
    });
  }

  ngOnInit() {
  }

}