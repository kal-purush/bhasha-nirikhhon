import { Injectable } from '@angular/core';
import {ReferenceLine, SensorMeasurementHistory} from "../model/measurementHistory.model";

@Injectable({
  providedIn: 'root'
})
export class ChartService {

  constructor() { }

  calculateYScaleMin(data: SensorMeasurementHistory[]): number {
    // TODO: Implement this method
    return 0;
  }

  calculateYScaleMax(data: SensorMeasurementHistory[]): number {
    // TODO: Implement this method
    return 40;
  }

  calculateReferenceLines(data: SensorMeasurementHistory[]): ReferenceLine[] {
    // TODO: Implement this method
    return [{name: "alarmMax", value: 30.0}, {name: "alarmMin", value: 18.0}];
  }
}