import { User } from "./entity/User.entity";
import * as express from "express";
import { Request, Response } from "express";
import PostgresDataSource from "./app-data-source";

// establish database connection
PostgresDataSource.initialize()
  .then(() => {
    console.log("Data Source has been initialized!");
  })
  .catch((err) => {
    console.error("Error during Data Source initialization:", err);
  });

// create and setup express app
const app = express();
app.use(express.json());

// register routes
app.get("/users", async function (req: Request, res: Response) {
  const users = await PostgresDataSource.getRepository(User).find();
  res.json(users);
});

app.get("/users/:id", async function (req: Request, res: Response) {
  const results = await PostgresDataSource.getRepository(User).findOneBy({
    id: +req.params.id,
  });
  return res.send(results);
});

app.post("/users", async function (req: Request, res: Response) {
  const user = await PostgresDataSource.getRepository(User).create(req.body);
  const results = await PostgresDataSource.getRepository(User).save(user);
  return res.send(results);
});

app.put("/users/:id", async function (req: Request, res: Response) {
  const user = await PostgresDataSource.getRepository(User).findOneBy({
    id: +req.params.id,
  });
  PostgresDataSource.getRepository(User).merge(user, req.body);
  const results = await PostgresDataSource.getRepository(User).save(user);
  return res.send(results);
});

app.delete("/users/:id", async function (req: Request, res: Response) {
  const results = await PostgresDataSource.getRepository(User).delete(
    req.params.id
  );
  return res.send(results);
});

// start express server
app.listen(3000);