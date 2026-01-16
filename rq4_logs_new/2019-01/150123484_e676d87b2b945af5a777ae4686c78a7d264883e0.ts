import { Component, OnInit, Input } from '@angular/core';
import { IDashboardState } from 'src/app/modules/dashboard/types/dashboard-state/dashboard-state.interface';

@Component({
  selector: 'app-dashboard-card',
  templateUrl: './DashboardCard.component.html',
  styleUrls: ['./dashboardCard.component.scss']
})
export class DashboardCardComponent {
  @Input() summary: IDashboardState;

  constructor() {}
}