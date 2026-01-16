import { prayerNamesArabic, prayerNamesEnglish } from "!globals";
import { convert12To24hr, convert24hrToMillisecond } from "!utils";
import { GetPrayersApiResponse, PrayerItem } from "!interfaces";

export class PrayersController {
  private _apiResult: GetPrayersApiResponse;
  private _prayers: PrayerItem[] = [];

  constructor(apiResult: GetPrayersApiResponse) {
    this._apiResult = apiResult;
  }

  // Private functions
  public processData = (): PrayerItem[] => {
    this._setPrayerConfigs();
    this._formatPrayerTimes();

    return this._prayers;
  };

  private _setPrayerConfigs = (): void => {
    const prayers: PrayerItem[] = prayerNamesEnglish.map((name, index): PrayerItem => {
      return {
        index,
        arabic: prayerNamesArabic[index],
        english: name,
        isNext: false,
        passed: false,
        time: this._apiResult[name.toLocaleLowerCase()]
      };
    });

    this._prayers = prayers;
  };

  private _formatPrayerTimes = (): void => {
    const prayers = this._prayers.map((prayer: PrayerItem): PrayerItem => {
      const military: string = convert12To24hr(prayer.english, prayer.time);
      const time: number = convert24hrToMillisecond(military);
      const now: number = new Date().getTime();

      prayer.time = military;
      prayer.passed = now > time;

      return prayer;
    });

    this._prayers = prayers;
  };
}