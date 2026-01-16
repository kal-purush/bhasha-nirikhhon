import { Component, Input, Output, EventEmitter } from '@angular/core';
import { IRevision } from 'app/shared/model/ICAMApi/revision.model';
import { IArticle } from 'app/shared/model/ICAMApi/article.model';
import { reviewStates } from '../status';

@Component({
  selector: 'bo-revision-card',
  templateUrl: 'revision-card.component.html',
  styleUrls: ['./revision-card.scss']
})
export class RevisionCardComponent {
  revision?: IRevision;
  @Input('revision') set _revision(revision: IRevision) {
    this.status = reviewStates.find(v => v.value === revision.reviewState)?.label;
    this.revision = revision;
  }
  @Input() article?: IArticle;
  @Output() cardClick = new EventEmitter<void>();
  @Output() onEdit = new EventEmitter<number>();
  status?: string = '';
}