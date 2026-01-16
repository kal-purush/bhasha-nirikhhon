import { DataTypes, Sequelize } from "sequelize";
import * as sequelize from "sequelize";
import { ModelContainer, StandardAttributes, StandardInstance } from "../model";

export interface RouteAttributes extends StandardAttributes {
  routeNumber?: number;
  vehicleType?: string;
}

export interface RouteInstance extends StandardInstance<RouteAttributes> {
   routeNumber: number;
   vehicleType: string;
}

export type RouteModel = sequelize.Model<RouteInstance, RouteAttributes>;

export default function (database: Sequelize, types: DataTypes): RouteModel {
   const route: RouteModel = database.define("Route", {
      routeNumber: {
        allowNull: false,
        type: types.INTEGER,
     },

     vehicleType: {
        allowNull: false,
        type: types.STRING,
     },
   });

   return route;
}