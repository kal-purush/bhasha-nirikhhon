import { Component, OnInit } from '@angular/core';
import {CommentService, Comment} from "../comment-list/comment/comment.service";

@Component({
  selector: 'app-comment-form',
  templateUrl: './comment-form.component.html',
  styleUrls: ['comment-form.component.scss'],
  providers: [CommentService]
})
export class CommentFormComponent implements OnInit {

  constructor(private service: CommentService) { }

  comment: Comment;


  ngOnInit() {
    this.comment = new Comment('','anonymous','', new Date());
  }

  onAddComment() {
    this.service.addComment(this.comment);
    this.comment = new Comment('','anonymous','', new Date());
  }
}