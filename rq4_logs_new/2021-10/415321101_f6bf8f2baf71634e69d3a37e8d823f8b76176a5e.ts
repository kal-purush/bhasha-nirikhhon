import dotenv from "dotenv";
dotenv.config();
import express from "express";
import { graphqlHTTP } from "express-graphql";
import schema from "./schema";
import root from "./root";
import authMiddleware from "./middleware";
const app = express();
app.use(authMiddleware);
app.use(
  "/graphql",
  graphqlHTTP({
    schema: schema,
    rootValue: root,
    graphiql: true,
  })
);
app.listen(4000, () => {
  console.log("Running a GraphQL API server at localhost:4000/graphql");
});