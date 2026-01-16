import {Component, OnInit} from '@angular/core';
import {StreamService} from '../stream.service';

@Component ({
  selector: 'search-stream',
  templateUrl: './search-stream.component.html',
  styleUrls: ['./search-stream.component.css']
})

export class SearchStreamComponent implements OnInit {


  constructor (private streamService: StreamService) {}

  ngOnInit () {

  }

}