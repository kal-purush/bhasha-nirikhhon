import { DataTypes, DefineAttributes } from "sequelize";
import { NewTableMigration } from "../NewTableMigration";

export = new NewTableMigration("RoutePoints", (types: DataTypes): DefineAttributes => ({
   index: {
      allowNull: false,
      type: types.INTEGER,
   },

   isReversed: {
      allowNull: false,
      type: types.BOOLEAN,
   },

   routeId: {
      allowNull: false,
      references: {
         model: "Routes",
         key: "id",
      },
      type: types.INTEGER,
   },

   stationId: {
      allowNull: false,
      references: {
         model: "Stations",
         key: "id",
      },
      type: types.INTEGER,
   },
}));