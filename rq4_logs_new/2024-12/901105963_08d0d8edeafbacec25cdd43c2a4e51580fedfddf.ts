interface ILocalNames {
    sv?: string;
    zh?: string;
    en?: string;
    ru?: string;
    vi?: string;
    fr?: string;
    ko?: string;
}
export interface IPlace {
    name?: string,
    local_names?: ILocalNames,
    lat: number,
    lon: number,
    country?: string
}

export interface IWeather {
    coord: {
      lon: number;
      lat: number;
    };
    weather: Array<{
      id: number;
      main: string;
      description: string;
      icon: string;
    }>;
    base: string;
    main: {
      temp: number;
      feels_like: number;
      temp_min: number;
      temp_max: number;
      pressure: number;
      humidity: number;
      sea_level?: number;
      grnd_level?: number;
    };
    visibility: number;
    wind: {
      speed: number;
      deg: number;
      gust?: number;
    };
    clouds: {
      all: number;
    };
    dt: number;
    sys: {
      type: number;
      id: number;
      country: string;
      sunrise: number;
      sunset: number;
    };
    timezone: number;
    id: number;
    name: string;
    cod: number;
  }
  

  export interface IDailyWeatherData {
    dt: number;
    sunrise: number;
    sunset: number;
    moonrise: number;
    moonset: number;
    moon_phase: number;
    temp: {
      day: number;
      min: number;
      max: number;
      night: number;
      eve: number;
      morn: number;
    };
    feels_like: {
      day: number;
      night: number;
      eve: number;
      morn: number;
    };
    pressure: number;
    humidity: number;
    dew_point: number;
    wind_speed: number;
    wind_deg: number;
    wind_gust: number;
    weather: Array<{
      id: number;
      main: string;
      description: string;
      icon: string;
    }>;
    clouds: number;
    pop: number;
    rain?: number;
    uvi: number;
  }

  export interface IAirPollution {
    main: {
      aqi: number
    },
    components: {
        co: number,
        no: number,
        no2: number,
        o3: number,
        so2: number,
        pm2_5: number,
        pm10: number,
        nh3: number
    },
    dt: number
}