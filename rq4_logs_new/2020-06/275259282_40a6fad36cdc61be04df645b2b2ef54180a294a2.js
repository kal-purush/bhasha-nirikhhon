import { Sequelize } from "sequelize"
const {
  dbAddress,
  dbName,
  dbPassword,
  dbPort,
  dbUsername,
} = require("./DatabaseDetails");

var state = {
    sequelize : ""
}

export const Connect = async (callback) => {  
    const sequelize = new Sequelize(dbName, dbUsername, dbPassword, {
        host: dbAddress,
        port: dbPort,
        dialect: "mysql",
      });
    
    try {
        await sequelize.authenticate();
        state.sequelize = sequelize;
        callback();
      } catch (error) {
        console.error('Unable to connect to the database:', error);
      }
};

export const getSequelize = () => {
    return state.sequelize;
}

export const syncDatabase = async () => {
    await state.sequelize.sync({ force: true });
}