import { ChangeDetectionStrategy, Component, Input, OnInit } from '@angular/core';

@Component({
  selector: 'app-md-view',
  templateUrl: './md-view.component.html',
  styleUrls: ['./md-view.component.scss'],
  changeDetection: ChangeDetectionStrategy.OnPush
})
export class MdViewComponent implements OnInit {

  @Input()
  text: string

  constructor() { }

  ngOnInit(): void {
  }

}