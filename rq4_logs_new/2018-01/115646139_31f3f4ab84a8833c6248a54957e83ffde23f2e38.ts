import { Component, OnInit } from '@angular/core';
import {RenderMyPageService} from '../../../services/render-my-page.service';

@Component({
  selector: 'app-wall',
  templateUrl: './wall.component.html',
  styleUrls: ['./wall.component.css']
})
export class WallComponent implements OnInit {
  posts;
  text;
  text;
  postsArray;
  constructor(private renderMyPageService: RenderMyPageService) { }

  ngOnInit() {
    this.text = 'ewrgwerg';
    this.postsArray = [];
    this.renderMyPageService.getPostsByKey(JSON.parse(localStorage.getItem('loggedUserKey')))
      .then(posts => {
        console.log('posts in me', posts);
        for (const prop in posts) {
          if (prop !== 'key') {
            console.log(posts[prop]);
            this.postsArray.push(posts[prop]);
          }

        }
        console.log(this.postsArray[1].post);

      });
    // this.renderMyPageService.getAllPosts(JSON.parse(localStorage.getItem('loggedUserKey')))
    //   .then(posts => {
    //     debugger;
    //     this.posts = posts;
    //   })
    //   .catch(err => console.log('ERR'));
    /*
    this.postsArray = [];
    this.renderMyPageService.getPostsByKey(JSON.parse(localStorage.getItem('loggedUserKey'))).then(posts => {
      this.posts = posts;

      // for (const i in this.posts) {
      //   this.postsArray.push(this.posts[i]);
      // }
      const keys = [];
      for (const post in this.posts) {
        keys.push(post);
      }

      const posts = [];
      for (let i = 0; i < keys.length; i++) {
        posts.push(this.posts[keys[i]].post);
      }
      console.log(posts)
      this.postsArray = posts.filter(Boolean);
      console.log(this.postsArray)

      // console.log(this.postsArray)
      // this.postsArray.shift();
    });
    console.log('postsArray', this.postsArray);
    // this.posts = [`The new syntax has a couple of things to note.The first is *ngFor.The * is a shorthand for using the new Angular template syntax with the template tag.`];
    // for ( let i = 0; i < 10; i++) {
    //   this.posts.push(this.posts[0]);
    // }
    */
  }

}