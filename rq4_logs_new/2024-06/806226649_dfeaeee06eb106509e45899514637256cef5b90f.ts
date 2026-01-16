import {PhysicalType} from "../src/app/shared/model/sensor-type.model";

export const sensorTypeMock = [
  {
    id: 1,
    name: "SHT30",
    type: PhysicalType.TEMPERATURE,
    maxValue: 120.0,
    minValue: -40.0,
  },
  {
    id: 2,
    name: "SHT30",
    type: PhysicalType.HUMIDITY,
    maxValue: 0.0,
    minValue: 100.0,
  },
  {
    id: 3,
    name: "MQ-2",
    type: PhysicalType.GAS,
    maxValue: 100,
    minValue: 10000,
  },
  {
    id: 4,
    name: "PMS5003",
    type: PhysicalType.PARTICLE,
    maxValue: 100,
    minValue: 10000,
  },
  {
    id: 5,
    name: "GY-BME280",
    type: PhysicalType.PRESSURE,
    maxValue: 300,
    minValue: 1100,
  },
  {
    id: 6,
    name: "BH1750",
    type: PhysicalType.LIGHT,
    maxValue: 1,
    minValue: 65536,
  },

]