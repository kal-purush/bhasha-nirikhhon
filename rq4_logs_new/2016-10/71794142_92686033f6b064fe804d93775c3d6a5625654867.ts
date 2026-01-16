import * as Sequelize from 'sequelize';
import path = require('path');
let env = process.env.NODE_ENV || 'development';
let config = require(path.join(__dirname, '..', '..', 'config', 'config.json'))[env];

export abstract class RepoBase{
   private static sequelize: Sequelize.Sequelize;

   public static getSequelize(): Sequelize.Sequelize{
       if(!this.sequelize){
           this.sequelize = new Sequelize(config.database, config.username, config.password, config.config);
       }
       return this.sequelize;
   }
}