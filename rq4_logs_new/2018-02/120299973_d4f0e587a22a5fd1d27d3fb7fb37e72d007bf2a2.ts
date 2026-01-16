import { Component, OnInit, OnDestroy } from '@angular/core';
import {ActivatedRoute} from '@angular/router';

import { Subscription } from 'rxjs/Subscription';
import {CommentSectionService} from './comment-section/comment-section.service';
import { aboutComment } from './comment-section/comment.model';

@Component({
  selector: 'app-about',
  templateUrl: './about.component.html',
  styleUrls: ['./about.component.css']
})
export class AboutComponent implements OnInit {

  comments: aboutComment[];
  private subscription: Subscription;



  constructor(private route: ActivatedRoute,
    private csService: CommentSectionService) { }

  ngOnInit() {


    this.comments = this.csService.getComments();
    this.subscription = this.csService.commentsChanged
    .subscribe(
      (comments: aboutComment[]) => {
        this.comments = comments;
      }
    );
  }

  onEditItem(index: number) {
    this.csService.startedEditing.next(index);

  }

  ngOnDestroy(){
    
    this.subscription.unsubscribe();
}

}