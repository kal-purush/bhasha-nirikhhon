import {Component, OnInit} from "@angular/core";
import { WeatherService } from './weather.service';
import { WeatherData, City } from '../shared/';

@Component({
  selector: 'city-card',
  template: require('./city-card.component.html'),
  styles: [require('./city-card.component.scss')],
  providers: [ WeatherService ]
})
export class CityCardComponent implements OnInit {
  weatherData: Promise<City[]>;
  isLoading: boolean = true;

  constructor(private weatherService: WeatherService) {}
  ngOnInit() {
    this.weatherData = this.getList();
  }

  getList() {
    return this.weatherService.getWeather()
      .then((data: WeatherData) => {
        this.isLoading = false;
        return data.list;
      })
  }
}