import { Component, OnInit } from '@angular/core';
import { FormBuilder, FormGroup } from '@angular/forms';
import { WeatherInformationService } from '../../../weather-information/services/weather-information.service';
import { SearchService } from '../../services/search.service';

@Component({
  selector: 'app-search',
  templateUrl: './search.component.html',
  styleUrls: ['./search.component.scss']
})
export class SearchComponent implements OnInit {

  public SearchForm: FormGroup;
  constructor(private _fb: FormBuilder, private searchService: SearchService) { }

  ngOnInit() {
    this.SearchForm = this._fb.group({
      searchCity: []
    });
  }

  getWeatherFromCity(payload) {
    this.searchService.getWeather(payload.searchCity);
  }

}