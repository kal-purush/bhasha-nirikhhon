import { Gpio } from 'pigpio';
import { colord, HsvColor } from 'colord';

import type { AccessoryPlugin, HAP, CharacteristicValue, Logging, Service } from 'homebridge';
type LEDCharacteristic = 'Brightness' | 'Hue' | 'Saturation';
export interface LedConfig {
  name: string;
  rPin: number;
  gPin: number;
  bPin: number;
}

const PWM_RANGE = 8000;
const GAMMA_COR = 2.8;

export class DioderAccessory implements AccessoryPlugin {
  private readonly Characteristic;

  private readonly rPin: Gpio;
  private readonly gPin: Gpio;
  private readonly bPin: Gpio;
  public readonly name: string;

  private readonly LEDservice: Service;
  private readonly informationService: Service;

  constructor(private readonly log: Logging, config: LedConfig, hap: HAP) {
    this.name = config.name;
    this.Characteristic = hap.Characteristic;

    this.rPin = new Gpio(config.rPin, { mode: Gpio.OUTPUT });
    this.gPin = new Gpio(config.gPin, { mode: Gpio.OUTPUT });
    this.bPin = new Gpio(config.bPin, { mode: Gpio.OUTPUT });
    this.rPin.pwmRange(PWM_RANGE);
    this.gPin.pwmRange(PWM_RANGE);
    this.bPin.pwmRange(PWM_RANGE);
    this.rPin.pwmWrite(0);
    this.gPin.pwmWrite(0);
    this.bPin.pwmWrite(0);
    this.log("PWM frequency:", this.rPin.getPwmFrequency());

    this.informationService = new hap.Service.AccessoryInformation()
      .setCharacteristic(hap.Characteristic.Manufacturer, "Silizia")
      .setCharacteristic(this.Characteristic.Model, "Fancy LED");

    this.LEDservice = new hap.Service.Lightbulb(config.name);
    this.LEDservice.getCharacteristic(this.Characteristic.On).onGet(this.getOn.bind(this)).onSet(this.setOn.bind(this));
    this.LEDservice.getCharacteristic(this.Characteristic.Brightness).onGet(this.getBrightness.bind(this)).onSet(this.setBrightness.bind(this));
    this.LEDservice.getCharacteristic(this.Characteristic.Hue).onGet(this.getHue.bind(this)).onSet(this.setHue.bind(this));
    this.LEDservice.getCharacteristic(this.Characteristic.Saturation).onGet(this.getSaturation.bind(this)).onSet(this.setSaturation.bind(this));
  }

  getServices(): Service[] {
    return [this.informationService, this.LEDservice];
  }

  identify(): void {
    this.log("Identify!"); // ToDo
  }

  getCharacteristicValue(c: LEDCharacteristic): number {
    return this.LEDservice.getCharacteristic(this.Characteristic[c]).value as number;
  }
  
  setOn(on: CharacteristicValue): void {
    this.log("setOn", on);
    if (on){
      let v = this.getCharacteristicValue('Brightness');
      if (v === 0){
        v = 100;
        this.LEDservice.getCharacteristic(this.Characteristic.Brightness).updateValue(v);
      }
      this.setHSV({ h: this.getCharacteristicValue('Hue'), s: this.getCharacteristicValue('Saturation'), v });
    } else {
      this.rPin.pwmWrite(0);
      this.gPin.pwmWrite(0);
      this.bPin.pwmWrite(0);
    }
  }

  getOn(): boolean {
    return this.rPin.getPwmDutyCycle() > 0 || this.gPin.getPwmDutyCycle() > 0 || this.bPin.getPwmDutyCycle() > 0;
  }

  setBrightness(v: CharacteristicValue): void {
    this.log("setBrightness", v);
    this.LEDservice.getCharacteristic(this.Characteristic.Brightness).updateValue(v);
    this.setHSV({ h: this.getCharacteristicValue('Hue'), s: this.getCharacteristicValue('Saturation'), v: v as number});
  }

  getBrightness(): number {
    return this.getHSV().v;
  }

  setHue(h: CharacteristicValue): void {
    this.log("setHue", h);
    this.LEDservice.getCharacteristic(this.Characteristic.Hue).updateValue(h);
    this.setHSV({ h: h as number, s: this.getCharacteristicValue('Saturation'), v: this.getCharacteristicValue('Brightness')});
  }

  getHue(): number {
    return this.getHSV().h;
  }

  setSaturation(s: CharacteristicValue): void {
    this.log("setSaturation", s);
    this.LEDservice.getCharacteristic(this.Characteristic.Saturation).updateValue(s);
    this.setHSV({ h: this.getCharacteristicValue('Hue'), s: s as number, v: this.getCharacteristicValue('Brightness')});
  }

  getSaturation(): number {
    return this.getHSV().s;
  }

  setHSV(c: HsvColor): void {
    const { r, g, b } = colord(c).toRgb();
    this.rPin.pwmWrite(Math.round(Math.pow(r / 255, GAMMA_COR) * PWM_RANGE));
    this.gPin.pwmWrite(Math.round(Math.pow(g / 255, GAMMA_COR) * PWM_RANGE));
    this.bPin.pwmWrite(Math.round(Math.pow(b / 255, GAMMA_COR) * PWM_RANGE));
    this.log(`set RGB to ${r}, ${g}, ${b}`)
  }

  getHSV(): HsvColor {
    return colord({ 
      r: Math.round(Math.pow(this.rPin.getPwmDutyCycle() / PWM_RANGE, 1 / GAMMA_COR) * 255),
      g: Math.round(Math.pow(this.gPin.getPwmDutyCycle() / PWM_RANGE, 1 / GAMMA_COR) * 255),
      b: Math.round(Math.pow(this.bPin.getPwmDutyCycle() / PWM_RANGE, 1 / GAMMA_COR) * 255)
    }).toHsv();
  }
}