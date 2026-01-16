import { SnakeNamingStrategy } from "typeorm-naming-strategies";

const { DataSource } = require("typeorm");

export const getDataSource = () => {
  const dataSource = new DataSource({
    type: "postgres",
    host: process.env.HOST,
    port: process.env.PORT,
    username: process.env.USERNAME,
    password: process.env.PASSWORD,
    database: process.env.DATABASE,
    entities: [`${__dirname}/entities/*.js`],
    namingStrategy: new SnakeNamingStrategy(),
  });

  if (!dataSource) {
    throw new Error("Error in creating DB Source!");
  }

  return dataSource;
};