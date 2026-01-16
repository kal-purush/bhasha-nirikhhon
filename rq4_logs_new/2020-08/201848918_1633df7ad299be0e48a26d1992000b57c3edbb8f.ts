import { SensorStatus } from './SensorStatus'

export class HumiditySensorStatus extends SensorStatus{
  // 温度状态
  public temperatureStatus: string = '';
  // 湿度状态
  public humidityStatus: string = '';
  constructor (status: string) {
    super(status)
    this.temperatureStatus = status.slice(2, 4);
    this.humidityStatus = status.slice(6, 8);
  }

  /**
   *
   * @param tmp 温度 十六进制
   */
  setTemperatureStatus (tmp: string): HumiditySensorStatus {
    this.temperatureStatus = this.adaptHex(tmp)
    return this
  }
  getTemperatureStatus (): string {
    return this.temperatureStatus
  }

  setHumidityStatus (hum: string): HumiditySensorStatus {
    this.humidityStatus = this.adaptHex(hum)
    return this
  }
  getHumidityStatus (): string {
    return this.humidityStatus
  }
}