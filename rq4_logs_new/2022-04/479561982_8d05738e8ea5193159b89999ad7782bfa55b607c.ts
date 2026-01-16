import { Component } from '@angular/core';

import { WeatherForecastApiService } from '@bp/weather-forecast/services';

@Component({
	selector: 'bp-root',
	templateUrl: './app.component.html',
	styleUrls: ['./app.component.scss'],
})
export class AppComponent {
	title = 'weather-forecast';
	readonly errors$ = this.weatherForecastApi.errors$;

	constructor(private readonly weatherForecastApi: WeatherForecastApiService) {}

	onErrorClose() {
		this.weatherForecastApi.onClearError();
	}

	onSearch(city: string) {
		this.weatherForecastApi.getWeather(city, 'daily').subscribe(resolve => {
			console.log(resolve);
		});
	}
}