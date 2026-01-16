import { Injectable } from '@angular/core'

@Injectable()
export class CityListService{

getCityData(){
    return CITYDATA

}
}

const CITYDATA = {
    list: [
      {
        coord: {lon: -0.13, lat: 51.51},
        weather: [{id: 300, main: 'Drizzle', description: 'light intensity drizzle', icon: '09d'}],
        base: 'stations',
        main: {temp: 7.17, pressure: 1012, humidity: 81, temp_min: 6, temp_max: 8},
        visibility: 10000,
        wind: {speed: 4.1, deg: 80},
        clouds: {all: 90},
        dt: 1485789600,
        sys: {type: 1, id: 5091, message: 0.0103, country: 'GB', sunrise: 1485762037, sunset: 1485794875},
        id: 2643743,
        name: 'London',
        cod: 200,
      },
      {
        coord:{lon:145.77,lat:-16.92},
        weather:[{id:803,main: 'Clouds' , description:'broken clouds',icon:'04n'}],
        base:'cmc stations',
        main:{temp:293.25,pressure:1019,humidity:83,temp_min:289.82,temp_max:295.37},
        wind:{speed:5.1,deg:150},
        clouds:{all:75},
        dt:1435658272,
        sys:{type:1,id:8166,message:0.0166,country:'AU',sunrise:1435610796,sunset:1435650870},
        id:2172797,
        name:"Cairns",
        cod:200,
      },

      {
        coord:{lon:145.77,lat:-16.92},
        weather:[{id:803,main: 'Rain' , description:'Light Rain',icon:'04n'}],
        base:'cmc stations',
        main:{temp:9,pressure:1019,humidity:83,temp_min:289.82,temp_max:295.37},
        wind:{speed:5.1,deg:150},
        clouds:{all:75},
        dt:1435658272,
        sys:{type:1,id:8166,message:0.0166,country:'AU',sunrise:1435610796,sunset:1435650870},
        id:2172797,
        name: "New York",
        cod:200,
      },

      {
        coord:{lon:145.77,lat:-16.92},
        weather:[{id:803,main: 'Snow' , description:'Heavy Snow',icon:'04n'}],
        base:'cmc stations',
        main:{temp:-5.9,pressure:1019,humidity:83,temp_min:289.82,temp_max:295.37},
        wind:{speed:5.1,deg:150},
        clouds:{all:75},
        dt:1435658272,
        sys:{type:1,id:8166,message:0.0166,country:'AU',sunrise:1435610796,sunset:1435650870},
        id:2172797,
        name:"Syracuse",
        cod:200,
      },

      {
        coord:{lon:145.77,lat:-16.92},
        weather:[{id:803,main: 'Sunny' , description:'Sunny Skies',icon:'04n'}],
        base:'cmc stations',
        main:{temp:12,pressure:1019,humidity:83,temp_min:289.82,temp_max:295.37},
        wind:{speed:5.1,deg:150},
        clouds:{all:75},
        dt:1435658272,
        sys:{type:1,id:8166,message:0.0166,country:'AU',sunrise:1435610796,sunset:1435650870},
        id:2172797,
        name:"Los Angeles",
        cod:200,
      },
    ]
  }