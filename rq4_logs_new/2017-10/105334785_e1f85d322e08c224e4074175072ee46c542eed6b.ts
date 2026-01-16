import { Component, OnInit, Input, OnChanges } from '@angular/core';
import { foreCastModel } from '../../models/foreCastModel';
import {ChartModule} from 'primeng/primeng';


@Component({
  selector: 'app-fore-cast',
  templateUrl: './fore-cast.component.html',
  styleUrls: ['./fore-cast.component.css']
})
export class ForeCastComponent implements OnInit {


  @Input() foreCastModel:foreCastModel;

  constructor() { }

  ngOnInit() {
  }

}