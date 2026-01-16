import { Component, OnInit } from '@angular/core';
import { Post } from 'src/app/models/Post';
import { PictresqueAPIService } from 'src/app/services/pictresque-api.service';
import { Router } from '@angular/router';

@Component({
  selector: 'app-posts',
  templateUrl: './posts.component.html',
  styleUrls: ['./posts.component.scss']
})
export class PostsComponent implements OnInit {
  posts: any;
  constructor(
    private apiService: PictresqueAPIService,
    private router: Router
  ) {
    this.apiService.getAllPosts().subscribe(posts => {
      this.posts = posts;
    });
  }

  goToDetails = e => {
    const id = e._id;
    this.router.navigate([`post/${id}`]);
  };

  ngOnInit() {}
}