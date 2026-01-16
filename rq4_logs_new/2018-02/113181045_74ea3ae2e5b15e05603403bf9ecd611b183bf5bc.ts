import { DataTypes, DefineAttributes, QueryInterface, SequelizeStatic } from "sequelize";
import { Log } from "./Log";
import { StandardAttributes, StandardInstance } from "./model";

export class PopulateTableSeeder<TAttributes extends StandardAttributes> {

   /**
    * A class used for easy creation of migrations that create new tables.
    * It already contains the ID, createdAt and updatedAt fields.
    * It also contains the definitions of the `up` and `down` method
    *
    * @param tableName The name of the table to be created
    * @param attributeInitializer A callback that returns the definitions of the table fields
    */
   constructor(
      private readonly tableName: string,
      private readonly attributeInitializer: () => Promise<TAttributes[]>) {
   }

   /**
    * Populate the table
    *
    * @param queryInterface The interface to the database
    * @param sequelize The database connection
    */
   public up(queryInterface: QueryInterface): Promise<void> {
      return this.clearTable(queryInterface).then((): Promise<TAttributes[]> => {
         Log.DB("Initializing the seed attributes...");
         return this.attributeInitializer();
      }).then((attributes: TAttributes[]): any => {
         Log.DB("Adding 'createdAt' and 'updatedAt' properties to the attributes...");
         for (const attribute of attributes) {
            attribute.createdAt = new Date();
            attribute.updatedAt = new Date();
         }

         Log.DB("Populating table '" + this.tableName + "' with " + attributes.length +
            " records...");
         return queryInterface.bulkInsert(this.tableName, attributes);
      }).then((): void => {
         Log.DB("Done populating table '" + this.tableName + "'");
      });
   }

   /**
    * Empty the table
    *
    * @param queryInterface The interface to the database
    * @param sequelize The database connection
    */
   public down(queryInterface: QueryInterface): Promise<void> {
      return this.clearTable(queryInterface);
   }

   private clearTable(queryInterface: QueryInterface): Promise<void> {
      Log.DB("Clearing table '" + this.tableName + "'...");
      return queryInterface.bulkDelete(this.tableName, {}).then((): void => {
         Log.DB("Done clearing table '" + this.tableName + "'");
      });
   }
}