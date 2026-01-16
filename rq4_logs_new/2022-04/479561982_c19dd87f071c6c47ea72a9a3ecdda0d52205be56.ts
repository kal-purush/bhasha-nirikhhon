import { ChangeDetectorRef, Component, OnDestroy, OnInit } from '@angular/core';
import { Mode, State } from '../../interfaces';
import { forkJoin, Subject, takeUntil } from 'rxjs';
import { WeatherForecastApiService } from '@bp/weather-forecast/services';
import { RouterService, StateService, StateTableService } from '../../services';

@Component({
	selector: 'bp-main',
	templateUrl: './main.component.html',
	styleUrls: ['./main.component.scss'],
})
export class MainComponent implements OnInit, OnDestroy {
	title = 'weather-forecast';

	isLoading = false;
	readonly errors$ = this._weatherForecastApi.errors$;

	mode: Mode = 'daily';
	private readonly _destroy$ = new Subject<boolean>();

	constructor(
		private readonly _cdr: ChangeDetectorRef,
		private readonly _weatherForecastApi: WeatherForecastApiService,
		private readonly _stateTable: StateTableService,
		private readonly _state: StateService,
		private readonly _router: RouterService
	) {}

	ngOnInit() {
		if (this._router.params.mode) {
			this._stateTable.setState(this._router.params.mode);
		}

		if (this._router.params.city) {
			this._router.params.city.split(',').map((city: string) => this._getWeather(city));
		}

		this._stateTable.state$.pipe(takeUntil(this._destroy$)).subscribe(state => {
			this.mode = state.mode;
			const cities = state.cities.map(city => city.name);
			this._router.setParams(state.mode, cities);
		});
	}

	onErrorClose() {
		this._weatherForecastApi.onClearError();
	}

	onSearch(city: string) {
		this._getWeather(city);
	}

	onChangeMode(mode: Mode) {
		this._stateTable.setState(mode);
	}

	private _getWeather(city: string) {
		this.isLoading = true;
		forkJoin({
			daily: this._weatherForecastApi.getWeather(city, 'daily'),
			hourly: this._weatherForecastApi.getWeather(city, 'hourly'),
		})
			.pipe(takeUntil(this._destroy$))
			.subscribe(weather => {
				const newState: State = {
					city: weather.daily.name,
					daily: weather.daily,
					hourly: weather.hourly,
				};

				this._state.setState(newState);
				this._stateTable.setState(this.mode, weather[this.mode]);
				this.isLoading = false;
				this._cdr.markForCheck();
			});
	}

	ngOnDestroy() {
		this._destroy$.next(true);
		this._destroy$.complete();
	}
}