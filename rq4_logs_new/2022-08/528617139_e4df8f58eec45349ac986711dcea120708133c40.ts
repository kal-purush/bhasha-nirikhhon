export interface GetPrayersReponse {
  city: string;
  date: string;
  fajr: string;
  fajr_jamat: string;
  sunrise: string;
  dhuhr: string;
  dhuhr_jamat: string;
  asr: string;
  asr_2: string;
  asr_jamat: string;
  magrib: string;
  magrib_jamat: string;
  isha: string;
  isha_jamat: string;
}

export class Api {
  static key = "2a99f189-6e3b-4015-8fb8-ff277642561d";
  static path = "times";
  static format = "json";
  static baseUrl = "https://www.londonprayertimes.com";

  static getPrayersUrl = `${this.baseUrl}/api/${this.path}?format=${this.format}&key=${this.key}`;

  static async get(): Promise<GetPrayersReponse> {
    const { data } = await useFetch<GetPrayersReponse>(this.getPrayersUrl);
    return data.value;
  }
}