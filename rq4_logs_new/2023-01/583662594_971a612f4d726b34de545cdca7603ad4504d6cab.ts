import { Component } from '@angular/core';
import { ActivatedRoute } from '@angular/router';
import { Observable } from 'rxjs';
import { ContentfultirdService } from '../services/contentfultirdservice';

@Component({
  selector: 'app-concert-post',
  templateUrl: './concert-post.component.html',
  styleUrls: ['./concert-post.component.css']
})
export class ConcertPostComponent {
  constructor(private contentfultirdService: ContentfultirdService, private route: ActivatedRoute) {}

  concertPosts$ : Observable<any> | undefined;

  ngOnInit(): void {
    this.route.params.subscribe(
      params => {
        const id = params['id'];
        this.concertPosts$ = this.contentfultirdService.getEntryByIdConcert(id);
      }
    )
    console.log(this.concertPosts$);
  }
}