import { Component, OnInit } from '@angular/core';
import { ApiServiceService } from '../../services/api-service.service';

@Component({
  selector: 'app-celebrities',
  templateUrl: './celebrities.component.html',
  styleUrls: ['./celebrities.component.css']
})
export class CelebritiesComponent implements OnInit {
  celebrities = [];

  constructor(private _apiService: ApiServiceService) { }

  ngOnInit() {
    this._apiService.getCelebrities().subscribe(
      resp => {
        resp = resp.json();
        console.log(resp.datos);
        this.celebrities = resp.datos;
      },
      error => {
        console.error(error);
      }
    );
  }

}