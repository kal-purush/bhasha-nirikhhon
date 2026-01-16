import { MongoClient, Db } from "mongodb";

let dbInstance: Db;
const MONGODB_URI = "mongodb://localhost:27017/test-database";

export const run = async () => {
  const client = new MongoClient(MONGODB_URI);
  await client.connect();

  dbInstance = client.db();

  const filter = "'Juan Pérez';sleep(5000)";
  const fn = 'function () { const search =' + filter + ';return this.name == search;}';

  const user = await dbInstance.collection("users").findOne({
    $where: fn
  });

  /*
  const user = await dbInstance.collection("users").findOne({
    $where: function () {
      const nombre = search;
      return this.nombre == nombre;
    },
  });*/

  console.log(user);

  await client.close();
};