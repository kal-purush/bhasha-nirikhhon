import { Clouds, Coord, Weather, Sys, Icon, Wind } from '../shared';

export class City {
  clouds: Clouds;
  coord: Coord;
  dt: number;
  id: number;
  main: Weather;
  name: string;
  sys: Sys;
  weather: Icon[];
  wind: Wind;

  constructor(
    clouds: Clouds,
    coord: Coord,
    dt: number,
    id: number,
    main: Weather,
    name: string,
    sys: Sys,
    weather: Icon[],
    wind: Wind
  ) {
    this.clouds = clouds;
    this.coord = coord;
    this.dt = dt;
    this.id = id;
    this.main = main;
    this.name = name;
    this.sys = sys;
    this.weather = weather;
    this.wind = wind;
  }
}