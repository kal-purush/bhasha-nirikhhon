import { Subject } from 'rxjs/Subject';
import { videoComment } from './comment-section/comment.model';


export class CommentSectionService {
    commentsChanged = new Subject<videoComment[]>();
    startedEditing = new Subject<number>();
    private comments: videoComment[] =[
        
       
        new videoComment('Nik', 'Good', 'video-1'),
        new videoComment('Tom', 'Great', 'video-1'),
        new videoComment('Tom', 'Bad', 'video-2'),
        new videoComment('Tom', '3 is Bad', 'video-3'),
      ];

      getComments() {
          return this.comments.slice();
      }

      getComment(index: number) {
          return this.comments[index];
      }

      addComment(comment: videoComment){
          this.comments.push(comment);
          this.commentsChanged.next(this.comments.slice())
      }

      addComments(comments: videoComment[]) {
        //    for (let ingredient of ingredients) {
        //        this.addIngredient(ingredient)
        //    }

        this.comments.push(...comments);
        this.commentsChanged.next(this.comments.slice())
      }

      updateComment(index: number, newVideoComment: videoComment){
          this.comments[index] = newVideoComment;
          this.commentsChanged.next(this.comments.slice());
      }

      deleteComment(index: number) {
          this.comments.splice(index, 1);
          this.commentsChanged.next(this.comments.slice());
      }
}