import { Sequelize, Options } from "sequelize";
import IDatabase from "./IDatabase";

class SequelizeDatabase implements IDatabase {
  instance: Sequelize
  constructor(dbName: string, dbUser: string, dbPass: string, dbConfig: Options) {

    console.log(dbName, dbUser, dbPass, dbConfig)
    this.instance = new Sequelize(dbName, dbUser, dbPass, dbConfig)
  }


  async hasConection() {
    try {
      await this.instance.authenticate();
      console.log("Banco dados conectado!");
    } catch (error) {
      console.error("Erro ao tentar se conectar ao banco de dados1");
    }
  }
}

export default SequelizeDatabase;