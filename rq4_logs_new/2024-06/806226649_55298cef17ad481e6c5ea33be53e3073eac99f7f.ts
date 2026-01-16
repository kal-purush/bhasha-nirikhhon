import { Injectable } from '@angular/core';
import {environment} from "../../../../environments/environment";
import {PhysicalType} from "../../../shared/model/sensor-type.model";

@Injectable({
  providedIn: 'root'
})
export class SeverityCalculationService {

  private readonly temperatureThreshold: number = environment.temperatureThreshold;
  private readonly humidityThreshold: number = environment.humidityThreshold;
  private readonly gasThreshold: number = environment.gasThreshold;

  constructor() { }

  getSeverity(type: PhysicalType | string, value: number, minAlarm: number, maxAlarm: number) {
    switch(type) {
      case PhysicalType.TEMPERATURE:
        return this.calculateSeverity(value, minAlarm, maxAlarm, this.temperatureThreshold);
      case PhysicalType.GAS:
        return this.calculateSeverity(value, minAlarm, maxAlarm, this.gasThreshold);
      case PhysicalType.HUMIDITY:
        return this.calculateSeverity(value, minAlarm, maxAlarm, this.humidityThreshold);
      default:
        return 'success';
    }
  }


  calculateSeverity(value: number, minAlarm: number, maxAlarm: number, threshold: number): string {
    if (this.checkDangerAlarm(value, minAlarm, maxAlarm)) {
      return 'danger';
    }

    if (this.checkWarningAlarm(value, minAlarm, maxAlarm, threshold)) {
      return 'warning';
    }
    return 'success';
  }

  private checkDangerAlarm(value: number, minAlarm: number, maxAlarm: number): boolean {
    return value <= minAlarm || value >= maxAlarm;
  }

  private checkWarningAlarm(value: number, minAlarm: number, maxAlarm: number, threshold: number): boolean {
    return value <= minAlarm + threshold || value >= maxAlarm - threshold;
  }


}