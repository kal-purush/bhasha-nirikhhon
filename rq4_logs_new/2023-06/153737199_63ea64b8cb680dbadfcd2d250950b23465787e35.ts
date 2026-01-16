import { z } from 'zod';
import { cloudsSchema, coordSchema, weatherSchema } from './common';

const citySchema = z.object({
  coord: coordSchema,
  country: z.string(),
  id: z.number(),
  name: z.string(),
  population: z.number(),
  timezone: z.number(),
});

const listSchema = z.object({
  clouds: cloudsSchema,
  dt: z.number(),
  dt_txt: z.string(),
  main: z.object({
    feels_like: z.number(),
    grnd_level: z.number(),
    humidity: z.number(),
    pressure: z.number(),
    sea_level: z.number(),
    temp_kf: z.number(),
    temp_max: z.number(),
    temp_min: z.number(),
    temp: z.number(),
  }),
  pop: z.number(),
  sys: z.object({
    pod: z.string(),
  }),
  visibility: z.number(),
  weather: z.array(weatherSchema),
  wind: z.object({
    deg: z.number(),
    gust: z.number(),
    speed: z.number(),
  }),
});

const forecastResponseSchema = z.object({
  city: citySchema,
  cnt: z.number(),
  cod: z.string(),
  list: z.array(listSchema),
  message: z.number(),
});

type ForecastResponse = z.infer<typeof forecastResponseSchema>;

export type { ForecastResponse };
export { forecastResponseSchema };