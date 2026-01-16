import {MeasurementHistory} from "../src/app/features/dashboard/model/measurementHistory.model";
import {PhysicalType} from "../src/app/shared/model/sensor-type.model";

export const temperatureMeasurementsHistory: MeasurementHistory[] = [
    {
      "name": "TEMPERATURE" as PhysicalType,
      "series": [
        {
          "value": 28,
          "name": new Date("2016-09-21T08:48:49.119Z"),
          "min": 15,
          "max": 30
        },
        {
          "value": 29,
          "name": new Date("2016-09-22T09:39:07.247Z"),
          "min": 15,
          "max": 30
        },
        {
          "value": 30,
          "name": new Date("2016-09-22T22:36:56.888Z"),
          "min": 15,
          "max": 30
        },
        {
          "value": 25,
          "name": new Date("2016-09-18T12:34:52.431Z"),
          "min": 15,
          "max": 30
        },
        {
          "value": 30,
          "name": new Date("2016-09-13T16:33:43.344Z"),
          "min": 15,
          "max": 30
        }
      ]
    }
  ]
;