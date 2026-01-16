import { DataSource } from "typeorm"

const AppDataSource = new DataSource({
  type: "postgres",
  host: "database",
  port: 5432,
  username: "docker",
  password: "segredo",
  database: "migoc2",
  "migrations": [
    "./src/database/migrations/*.ts"
  ],
  entities: [
    "./src/modules/**/entities/*.ts"
  ]
})

export { AppDataSource }

  // .then(() => {
  //   console.log("Data Source has been initialized!")
  // })
  // .catch((err) => {
  //   console.error("Error during Data Source initialization", err)
  // })
