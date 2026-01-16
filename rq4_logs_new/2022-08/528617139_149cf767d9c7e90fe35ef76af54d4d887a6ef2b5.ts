import { IBaseApi } from "!api/interfaces";
import { ILondonPrayerTimesApi } from "!api/LondonPrayerTimes/interfaces";

class LondonPrayerTimesApi implements IBaseApi {
  private _key = "2a99f189-6e3b-4015-8fb8-ff277642561d";
  private _path = "times";
  private _format = "json";
  private _baseUrl = "https://www.londonprayertimes.com";
  private _fullPrayerTimesUrl = `${this._baseUrl}/api/${this._path}?format=${this._format}&key=${this._key}`;

  public async get(): Promise<ILondonPrayerTimesApi> {
    const { data } = await useFetch<ILondonPrayerTimesApi>(this._fullPrayerTimesUrl);
    return data.value;
  }
}

export default LondonPrayerTimesApi;