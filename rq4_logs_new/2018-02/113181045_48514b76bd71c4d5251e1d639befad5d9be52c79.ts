import { Promise } from "bluebird";
import * as fs from "fs";
import * as path from "path";
import { QueryInterface, SequelizeStatic } from "sequelize";
import { Route, Station } from "../models";
import { RouteInstance } from "../models/route";
import { RoutePointAttributes } from "../models/route-point";
import { StationInstance, } from "../models/station";
import { PopulateTableSeeder } from "../PopulateTableSeeder";

interface RouteInfo {
   routeNumber: number;
   vehicleType: string;
   _routePoints: string[];
}

export = new PopulateTableSeeder<RoutePointAttributes>("RoutePoints", (): Promise<RoutePointAttributes[]> => {
   const filePath: string = path.resolve(__dirname, "../../../resources/routes.json");
   const resources: RouteInfo[] = JSON.parse(fs.readFileSync(filePath).toString());
   const stationMap: { [stationNumber: string]: StationInstance } = {};
   const routeMap: { [customRouteId: string]: RouteInstance } = {};

   return Station.findAll().then((stations: StationInstance[]): Promise<RouteInstance[]> => {
      for (const station of stations) {
         stationMap[station.stationNumber] = station;
      }
      return Route.findAll();
   }).then((routes: RouteInstance[]): RoutePointAttributes[] => {
      for (const route of routes) {
         routeMap[route.vehicleType + "-" + route.routeNumber] = route;
      }

      const attributes: RoutePointAttributes[] = [];
      for (const route of resources) {
         const routeCustomId: string = route.vehicleType + "-" + route.routeNumber;
         const targetRoute: RouteInstance = routeMap[routeCustomId];
         if (!targetRoute) {
            throw new Error("Could not find route '" + routeCustomId + "'");
         }
         let subrouteIndex: number = 0;
         for (const subroute of route._routePoints) {
            const targetStationNumbers: string[] = subroute.split(",");

            let routePointIndex: number = 0;
            for (const targetStationNumber of targetStationNumbers) {
               const targetStation: StationInstance = stationMap[targetStationNumber];
               if (!targetStation) {
                  throw new Error("Could not find station '" + targetStationNumber + "'");
               }

               attributes.push({
                  index: routePointIndex,
                  subrouteIndex,
                  routeId: targetRoute.id,
                  stationId: targetStation.id,
               });
               routePointIndex++;
            }
            subrouteIndex++;
         }
      }
      return attributes;
   });
});