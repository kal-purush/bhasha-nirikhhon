import Api from "~~/api/abstract";
import { IApi } from "~~/api/LondonPrayerTimesApi/interfaces";

class LondonPrayerTimesApi implements Api {
  private _key = "2a99f189-6e3b-4015-8fb8-ff277642561d";
  private _path = "times";
  private _format = "json";
  private _baseUrl = "https://www.londonprayertimes.com";
  private _fullUrl: string;

  constructor() {
    this._fullUrl = `${this._baseUrl}/api/${this._path}?format=${this._format}&key=${this._key}`;
  }

  public async fetchPrayerTimes(): Promise<IApi> {
    const response: Response = await fetch(this._fullUrl);
    return response.json();
  }
}

export default LondonPrayerTimesApi;