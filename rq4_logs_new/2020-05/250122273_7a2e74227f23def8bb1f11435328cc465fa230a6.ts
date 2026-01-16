import { Component, OnInit } from '@angular/core';
import { Router } from '@angular/router';
import { RevisionService } from 'app/entities/ICAMApi/revision/revision.service';
import { IRevision } from 'app/shared/model/ICAMApi/revision.model';
import { EMPTY, Observable } from 'rxjs';
import { map } from 'rxjs/operators';
import { ReviewState } from 'app/shared/model/enumerations/review-state.model';

@Component({
  selector: 'revision-list',
  templateUrl: 'revision-list.component.html',
  styleUrls: ['../article/article-list/article-list.scss']
})
export class RevisionListComponent implements OnInit {
  revisions: Observable<IRevision[]> = EMPTY;
  state?: ReviewState;
  states = [ReviewState.Hold, ReviewState.OnGoing, ReviewState.Pending, ReviewState.Reviewed, ReviewState.Accepted];
  constructor(private revisionService: RevisionService, private router: Router) {}

  ngOnInit(): void {
    this.loadData();
  }

  loadData(state: ReviewState | undefined = this.state): void {
    this.state = state;
    let params;
    if (state) {
      params = {
        'reviewState.equals': state
      };
    }
    this.revisions = this.revisionService.query(params).pipe(map(r => r.body || []));
  }

  updateRevisionState(reviewState: ReviewState, revision: IRevision): void {
    this.revisionService.update({ ...revision, reviewState }).subscribe(() => this.loadData());
  }
}