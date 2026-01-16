import { Injectable } from '@angular/core';
import { ConfigService } from './config.service';

@Injectable({
  providedIn: 'root'
})

export class AlarmService {
  hour: string = "";
  minute: string = "";
  timeOut: any;
  alarmOn: boolean = false;
  isActive: boolean = false;
  isChecked: boolean = false;
  color: string = "grey";
  colorToggle: string = "primary";
  alarmSound = new Audio(this.configService.urlSoundAlarm);

  constructor(private configService: ConfigService) { }

  startArarm() {
    this.alarmOn = true;
    this.color = "blue";
    let ms: number = this.calculateSecond();
    let alarm = new Date(ms);
    let differenceInMs = alarm.getTime() - (new Date().getTime());
    this.timeOut = setTimeout(this.playAlarm.bind(this), differenceInMs);
  }

  calculateSecond() {
    let hours = Number(this.hour);
    let minutes = Number(this.minute);
    let currentTime = new Date();
    let date = new Date();
    date.setHours(hours);
    date.setMinutes(minutes);
    date.setSeconds(0);
    if ((currentTime.getHours() > hours) || (currentTime.getMinutes() >= minutes)) {
      date.setDate(date.getDate() + 1);
    }
    return date.getTime();
  }

  playAlarm() {
    this.alarmSound.loop = true;
    this.alarmSound.play();
    this.isActive = true;
    this.color = "red";
  }

  dismissAlarm() {
    if (this.isActive) {
      this.alarmSound.pause();
      this.alarmSound.currentTime = 0;
      this.isActive = false;
    } else {
      clearTimeout(this.timeOut);
      this.timeOut = 0;
    }
    this.color = "grey";
    this.alarmOn = false;
  }

  refresh() {
    this.hour = "";
    this.minute = "";
  }

  switchTimeShow() {
    if (this.isChecked) {
      this.isChecked = false;
    } else {
      this.isChecked = true;
    }
  }

}