import {Component, Input, OnInit} from "@angular/core";
import { WeatherData } from "../../shared";


@Component({
  selector: 'city-card',
  template: require('./city-card.component.html'),
  styles: [require('./city-card.component.scss')]
})
export class CityCardComponent implements OnInit {
  @Input() cityWeather: WeatherData;
  @Input() search: string = '';

  ngOnInit() {}
}