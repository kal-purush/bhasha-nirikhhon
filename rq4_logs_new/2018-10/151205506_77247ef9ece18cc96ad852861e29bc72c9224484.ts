import { TrackingCategory } from './../../models';
import { Component, OnInit, Input } from '@angular/core';
import { HistoryLog } from '../../models';

@Component({
  selector: 'trk-history-log',
  templateUrl: './history-log.component.html',
  styleUrls: ['./history-log.component.scss']
})
export class HistoryLogComponent implements OnInit {
  @Input() history: HistoryLog;
  @Input() category: TrackingCategory;

  constructor() { }

  ngOnInit() {
  }

}