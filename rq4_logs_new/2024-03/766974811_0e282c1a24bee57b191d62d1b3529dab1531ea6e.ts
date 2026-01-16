import { DataSource } from "typeorm";

const SqlDataSource = new DataSource({
    host: "localhost",
    port: 3306,
    username: "root",
    password: "123456",
    database: "simpatech",
    type: "mysql",
    synchronize: false,
    logging: false,
    entities: ["src/entities/*.ts"],
    migrations: ["src/migrations/*.ts"],
});


SqlDataSource.initialize()
    .then(() => {
        console.log("Data Source inicializado!");
    })
    .catch((e) => {
        console.error("Erro na inicialização do Data Source:", e);
    });

export default SqlDataSource;