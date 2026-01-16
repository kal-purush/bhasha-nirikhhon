import { prayerNamesArabic, prayerNamesEnglish } from "~~/readonly";
import convert12To24hr from "!utils/convert12To24hr";
import convert24hrToMillisecond from "!utils/convert24hrToMillisecond";
import { IPrayer } from "!interfaces";
// import { ILondonPrayerTimesApi } from "!api/LondonPrayerTimes/interfaces";
// import LondonPrayerTimeApi from "!api/LondonPrayerTimes/api";
import { IBaseController } from "./interfaces";

class LondonPrayerTimeController implements IBaseController {
  // private _api = new LondonPrayerTimeApi();
  private store: any;

  constructor(Store: any) {
    this.store = Store;
  }

  // Public functions
  public buildPrayerTimes = async (): Promise<void> => {
    // this._rawData = await this._api.get();

    this.store.apiResult = {
      fajr: "02:56",
      sunrise: "02:57"
      // dhuhr: "00:00",
      // asr: "00:05",
      // magrib: "00:48",
      // isha: "00:49"
    };

    this._processPrayerTimes();
  };

  public setNextPrayerIndex = (): void => {
    this.store.nextPrayerIndex = this.store.prayers.findIndex((prayer: IPrayer) => !prayer.passed);

    console.log(
      "🚀 > file: LondonPrayerTimes.ts > line 35 > LondonPrayerTimeController > this.store.nextPrayerIndex",
      this.store.nextPrayerIndex
    );

    if (this.store.nextPrayerIndex < 0) return;

    this.store.prayers[this.store.nextPrayerIndex].isNext = true;

    console.log("setNextPrayerIndex doneeee");
  };

  // Private functions
  private _processPrayerTimes = (): void => {
    this._setPrayerConfigs();
    this._formatPrayerTimes();
    // this.setNextPrayerIndex();
  };

  private _setPrayerConfigs = (): void => {
    const prayers: IPrayer[] = prayerNamesEnglish.map((name, index): IPrayer => {
      return {
        index,
        arabic: prayerNamesArabic[index],
        english: name,
        isNext: false,
        passed: false,
        time: this.store.apiResult[name.toLocaleLowerCase()]
      };
    });

    this.store.prayers = prayers;
  };

  private _formatPrayerTimes = (): void => {
    const prayers = this.store.prayers.map((prayer: IPrayer): IPrayer => {
      const military: string = convert12To24hr(prayer.english, prayer.time);
      const time: number = convert24hrToMillisecond(military);
      const now: number = new Date().getTime();

      prayer.time = military;
      prayer.passed = now > time;

      return prayer;
    });

    this.store.prayers = prayers;
  };
}

export default LondonPrayerTimeController;