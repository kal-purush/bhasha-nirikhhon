export interface IClockDate{
    Date: Date;
    short(): string;
    getTime(): number;
    isBefore(date: IClockDate): boolean;
    isAfter(date: IClockDate): boolean;
    addDays(days: number): IClockDate;
    addHours(hours: number): IClockDate;
    addMinutes(minutes: number): IClockDate;
    addSeconds(seconds: number): IClockDate;
    addMilliSeconds(milliseconds: number): IClockDate;
    fromTicks(ticks: number): IClockDate;
}

// Symbol used during runtime for dependency injection
export const IClockDate = Symbol('IClockDate')