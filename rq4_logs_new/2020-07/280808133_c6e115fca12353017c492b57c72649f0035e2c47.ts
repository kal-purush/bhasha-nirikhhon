import { Injectable } from '@angular/core';
import { Observable, Subject } from 'rxjs';

@Injectable()
export class CommentsStore {
  private expandedComments$: Subject<string> = new Subject<string>();
  private commentsExpandState: { [key: string]: boolean } = {};

  public expandedCommentsAsObservable(): Observable<string> {
    return this.expandedComments$.asObservable();
  }

  public isExpanded(commentId: string) {
    return !!this.commentsExpandState[commentId];
  }

  public expand(commentId: string) {
    this.commentsExpandState[commentId] = true;
    this.expandedComments$.next(commentId);
  }

  public collapse(commentId: string) {
    this.commentsExpandState[commentId] = false;
  }
}