import {Sensor} from "../src/app/features/settings/model/sensor.model";
import {PhysicalType} from "../src/app/shared/model/sensor-type.model";

export const sensorListMock: Sensor[] = [
  {
    "id": 1,
    "uuid": "uuid-1",
    "sensorBase": "AZEnvy",
    "sensorType": "TEMPERATURE" as PhysicalType,
    "sensorName": "SHT30",
    "sensorBaseSensorTypeId": 1,
    "userId": 1,
    "location": "Wohnzimmer",
    "alarmMin": 18.0,
    "alarmMax": 30.0,
    "alarmActive": true,
    "warningThreshold": 1.0,
    "linearCorrectionValue": 0.0,
    "created": new Date(2024, 5, 16, 8, 0),
    "updated": new Date(2024, 5, 16, 8, 0)
  },
  {
    "id": 2,
    "uuid": "uuid-2",
    "sensorBase": "AZEnvy",
    "sensorType": "HUMIDITY" as PhysicalType,
    "sensorName": "SHT30",
    "sensorBaseSensorTypeId": 2,
    "userId": 1,
    "location": "Wohnzimmer",
    "alarmMin": 30.0,
    "alarmMax": 65.0,
    "alarmActive": true,
    "warningThreshold": 5.0,
    "linearCorrectionValue": 0.0,
    "created": new Date(2024, 5, 16, 8, 0),
    "updated": new Date(2024, 5, 16, 8, 0)
  },
  {
    "id": 3,
    "uuid": "uuid-3",
    "sensorBase": "AZEnvy",
    "sensorType": "VOC" as PhysicalType,
    "sensorName": "MQ2",
    "sensorBaseSensorTypeId": 3,
    "userId": 1,
    "location": "Wohnzimmer",
    "alarmMin": 0.0,
    "alarmMax": 400.0,
    "alarmActive": false,
    "warningThreshold": 50.0,
    "linearCorrectionValue": 0.0,
    "created": new Date(2024, 5, 16, 8, 0),
    "updated": new Date(2024, 5, 16, 8, 0)
  }
]