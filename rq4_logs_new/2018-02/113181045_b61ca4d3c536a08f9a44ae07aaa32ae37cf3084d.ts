import { Promise } from "bluebird";
import * as fs from "fs";
import * as path from "path";
import { QueryInterface, SequelizeStatic } from "sequelize";
import { StationAttributes, } from "../models/station";
import { PopulateTableSeeder } from "../PopulateTableSeeder";

interface StationInfo {
   y: number;
   c: string;
   x: number;
   n: string;
}

export = new PopulateTableSeeder<StationAttributes>("Stations", (): Promise<StationAttributes[]> => {
   const filePath: string = path.resolve(__dirname, "../../../resources/stations.json");
   const resources: StationInfo[] = JSON.parse(fs.readFileSync(filePath).toString());

   return Promise.resolve(resources.map((info: StationInfo): StationAttributes => {
      return {
         name: info.n,
         stationNumber: info.c,
         longtitude: info.x,
         latitude: info.y,
         conductorAt: 0,
      };
   }));
});