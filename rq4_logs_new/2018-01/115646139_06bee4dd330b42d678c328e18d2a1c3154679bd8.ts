import { Component, OnInit, Input } from '@angular/core';
import {SearchService} from '../../services/search.service';

@Component({
  selector: 'app-found-list',
  templateUrl: './found-user.component.html',
  styleUrls: ['./found-user.component.css']
})
export class FoundUserComponent implements OnInit {
  constructor(protected searchService: SearchService) { }

  ngOnInit() {
  }

}