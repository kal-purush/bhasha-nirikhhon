import { Component, OnInit } from '@angular/core';

import { GenericContainerComponent } from '../generic-container.component';

@Component({
  selector: 'app-detail-view',
  templateUrl: './detail-view.component.html',
  styleUrls: ['./detail-view.component.css']
})
export class DetailViewComponent extends GenericContainerComponent implements OnInit {

  ngOnInit() {
    super.ngOnInit();
  }
}