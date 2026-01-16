import { Component, OnInit } from '@angular/core';
import { ActivatedRoute } from '@angular/router';
import { UserService } from '../../../providers/user.service';
import { DocumentSnapshot, QuerySnapshot, QueryDocumentSnapshot } from '@angular/fire/firestore';
import { PeliUser } from '../../../interfaces/peli-user.interface';
import { LikedMovie } from '../../../interfaces/liked-movie.interface';

@Component({
  selector: 'app-liked-movies',
  templateUrl: './liked-movies.component.html',
  styles: []
})
export class LikedMoviesComponent implements OnInit {

  likedMovies: LikedMovie[];

  constructor(private activatedRoute: ActivatedRoute,
              private userService: UserService) {
    this.activatedRoute.parent.params.subscribe(params => {
      this.getUser(params['uid'])
    });
  }

  ngOnInit() {
  }

  getUser = (uid: string) => {
    this.likedMovies = [];
    this.userService.getLikedMovies(uid).subscribe((user: QuerySnapshot<PeliUser>) => {
      user.forEach((movie: QueryDocumentSnapshot<LikedMovie>) => {
        this.likedMovies.push(movie.data());
      })
    })
  }

}