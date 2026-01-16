import { Duration, TimerData } from '@app/models/clock';

export function buildRunningTimer(startTime?: number): TimerData {
  return {
    isRunning: true,
    startTime: startTime || new Date(2016, 0, 1, 14, 0, 0).getTime(),
    duration: Duration.zero().toJSON()
  };
}

export function buildStoppedTimer(elapsedSeconds?: number): TimerData {
  return {
    isRunning: false,
    startTime: undefined,
    duration: (elapsedSeconds ? Duration.create(elapsedSeconds) : Duration.zero()).toJSON()
  };
}