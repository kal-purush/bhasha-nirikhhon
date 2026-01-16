import clone from "lodash.clonedeep";
import { prayerNamesArabic, prayerNamesEnglish } from "!globals";
import { GetPrayersApiResponse, PrayerItem } from "!interfaces";
import { TimerController } from "!controllers/Timer";

export class PrayerController {
  public static nextPrayerIndex = (prayersList: PrayerItem[]): number => {
    return prayersList.findIndex((prayer) => !prayer.passed);
  };

  public static processNextPrayer = (prayersList: PrayerItem[]): PrayerItem[] => {
    const prayers = clone(prayersList) as PrayerItem[];
    const nextPrayerIndex = prayers.findIndex((prayer) => !prayer.passed);

    if (nextPrayerIndex === -1) return prayers;

    prayers[nextPrayerIndex].isNext = true;

    return prayers;
  };

  public static processPreviousPrayer = (prayersList: PrayerItem[]): PrayerItem[] => {
    const prayers = clone(prayersList) as PrayerItem[];
    const nextPrayerIndex = prayers.findIndex((prayer) => !prayer.passed);

    if (nextPrayerIndex === -1) return prayers;

    prayers[nextPrayerIndex].passed = true;
    prayers[nextPrayerIndex].isNext = false;

    return prayers;
  };

  public static processApiResult = (apiResult: GetPrayersApiResponse): PrayerItem[] => {
    const prayers = prayerNamesEnglish.map((name, index): PrayerItem => {
      const prayer = {
        index,
        arabic: prayerNamesArabic[index],
        english: name,
        isNext: false,
        passed: false,
        time: apiResult[name.toLocaleLowerCase()]
      };

      const military = TimerController.convert12To24hr(prayer.english, prayer.time);
      const time = TimerController.convert24hrToMillisecond(military);
      const now = new Date().getTime();

      prayer.time = military;
      prayer.passed = now > time;

      return prayer;
    });

    return prayers;
  };
}