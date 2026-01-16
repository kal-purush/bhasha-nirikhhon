import { Component } from '@angular/core';

import 'rxjs/operator/count';

import { IssueCountService } from './issue-count.service';

@Component({
  selector: 'app-issue-count',
  templateUrl: './issue-count.component.html',
  styleUrls: ['./issue-count.component.css']
})
export class IssueCountComponent {

  public openIssueCount: number;

  constructor(private countSvc: IssueCountService) {
    this.countSvc.getOpenIssueCount()
      .subscribe(issueCount => this.openIssueCount = issueCount);
  }
}