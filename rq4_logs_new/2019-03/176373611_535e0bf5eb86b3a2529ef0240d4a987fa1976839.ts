import {City} from "./city.model";
import {WeatherConditions, WeatherConditionsAdapter} from "./weather-conditions.model";
import {Adapter} from "../../core/adapter";

export class CityForecast {
  city: City;

  constructor(
    city_id: number,
    futureConditions: WeatherConditions[]
  ) {
  }
}

export class CityForecastAdapter implements Adapter<CityForecast> {
  adapt(input: any): CityForecast {
    let weatherConditionsAdapter = new WeatherConditionsAdapter();
    let conditionsArray: WeatherConditions[] = [];
    for (let item of input.list) {
      let conditions = weatherConditionsAdapter.adapt(item);
      conditionsArray.push(conditions)
    }
    return new CityForecast(
      input.city.id,
      conditionsArray
    )
  }
}