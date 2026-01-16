import { Component, OnDestroy, OnInit } from '@angular/core';
import { FormControl } from '@angular/forms';
import { Subscription } from 'rxjs';
import { DateTime, Duration } from 'luxon/src/luxon';
import { Resolution, WeatherService } from '../../../core/data/weather';

@Component({
  selector: 'app-weather-time-selector',
  templateUrl: './weather-time-selector.component.html',
  styleUrls: ['./weather-time-selector.component.scss']
})
export class WeatherTimeSelectorComponent implements OnInit, OnDestroy {

  public range = new FormControl(24);
  public custom = false;
  private subscription?: Subscription;

  constructor(
    private readonly weatherService: WeatherService
  ) {
  }

  ngOnInit(): void {
    this.subscription = this.range.valueChanges.subscribe(hours =>
      this.weatherService.timeRange = {
        from: DateTime.now().minus(Duration.fromObject({ hours })),
        resolution: Resolution.HOURLY
      }
    );
  }

  ngOnDestroy(): void {
    this.subscription?.unsubscribe();
  }
}