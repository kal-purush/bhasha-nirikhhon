import {LatestMeasurement} from "../src/app/features/dashboard/model/measurement.model";

export const latestMeasurements: LatestMeasurement[] = [
  {
    "uuid": "F0F0F0",
    "id": 1,
    "sensorBaseName": "AZEnvy",
    "sensorName": "SHT30",
    "sensorType": "TEMPERATURE",
    "location": "Wohnzimmer",
    "alarmMax": 25.0,
    "alarmMin": 19.0,
    "timestamp": new Date(2024, 5, 5, 6, 5),
    "unit": "CELSIUS",
    "value": 26.5
  },
  {
    "uuid": "F0F0F0",
    "id": 2,
    "sensorBaseName": "AZEnvy",
    "sensorName": "SHT30",
    "sensorType": "Humidity",
    "location": "Wohnzimmer",
    "alarmMax": 65.0,
    "alarmMin": 45.0,
    "timestamp": new Date(2024, 5, 5, 6, 5),
    "unit": "PERCENT",
    "value": 45.5
  },
  {
    "uuid": "F0F0F0",
    "id": 3,
    "sensorBaseName": "AZEnvy",
    "sensorName": "MQ-2",
    "sensorType": "GAS",
    "location": "Wohnzimmer",
    "alarmMax": 200.0,
    "alarmMin": 0.0,
    "timestamp": new Date(2024, 5, 5, 6, 5),
    "unit": "PPM",
    "value": 201.0
  },
  {
    "uuid": "F0F0F1",
    "id": 4,
    "sensorBaseName": "AZEnvy",
    "sensorName": "SHT30",
    "sensorType": "TEMPERATURE",
    "location": "Schlafzimmer",
    "alarmMax": 20.0,
    "alarmMin": 17.0,
    "timestamp": new Date(2024, 5, 5, 6, 5),
    "unit": "CELSIUS",
    "value": 26.5
  },
  {
    "uuid": "F0F0F1",
    "id": 5,
    "sensorBaseName": "AZEnvy",
    "sensorName": "SHT30",
    "sensorType": "Humidity",
    "location": "Schlafzimmer",
    "alarmMax": 65.0,
    "alarmMin": 45.0,
    "timestamp": new Date(2024, 5, 5, 6, 5),
    "unit": "PERCENT",
    "value": 55.5
  },
  {
    "uuid": "F0F0F1",
    "id": 6,
    "sensorBaseName": "AZEnvy",
    "sensorName": "MQ-2",
    "sensorType": "GAS",
    "location": "Schlafzimmer",
    "alarmMax": 270.0,
    "alarmMin": 0.0,
    "timestamp": new Date(2024, 5, 5, 6, 5),
    "unit": "PPM",
    "value": 260.5
  }
];