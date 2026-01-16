import { Status } from './Status'
export class SensorStatus extends Status{
  rootStatus: string = ''
  cardStatus: string = ''
  temperatureStatus: string = ''
  humidityStatus: string = ''

  constructor (status:string) {
    super(status)
    this.rootStatus = status.slice(8, 10)
    this.cardStatus = status.slice(0, 2)
    this.temperatureStatus = status.slice(2, 4)
    this.humidityStatus = status.slice(6, 8)
  }

  getSensorRootStatus () {
    return this.rootStatus
  }
  getSensorCardStatus () {
    return this.cardStatus
  }
  getSensorTemperatureStatus () {
    return this.temperatureStatus
  }
  getSensorHumidityStatus () {
    return this.humidityStatus
  }
}