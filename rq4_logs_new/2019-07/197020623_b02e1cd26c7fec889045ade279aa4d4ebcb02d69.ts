import { ChangeDetectionStrategy, Component, OnInit } from '@angular/core';
import { DraftFacade } from 'src/app/draft/+state/draft.facade';

@Component({
  selector: 'app-dashboard',
  templateUrl: './dashboard.component.html',
  styleUrls: ['./dashboard.component.less'],
  changeDetection: ChangeDetectionStrategy.OnPush
})
export class DashboardComponent implements OnInit {
  myTeam$ = this.facade.myTeam$;
  draftConfig$ = this.facade.draftConfig$;
  draftPickState$ = this.facade.draftPickState$;

  constructor(private facade: DraftFacade) { }

  ngOnInit() {
  }

}