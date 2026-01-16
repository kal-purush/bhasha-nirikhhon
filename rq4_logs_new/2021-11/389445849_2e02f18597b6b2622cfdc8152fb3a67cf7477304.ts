/* eslint-disable no-console */
import { connection } from "@/infra/database/postgres";

const date = new Date();
const localTime = date.toLocaleTimeString();

connection.create().then(async () => {
  console.log(
    `\x1b[32m[DATABASE] \x1b[30m${localTime}\x1b[32m database on\x1b[0m`
  );
  const { app } = await import("./config/app");
  const server = app.listen(3333, () =>
    console.log(
      `\x1b[32m[SERVER] \x1b[30m${localTime}\x1b[32m server on at http://localhost:3333\x1b[0m`
    )
  );
  process.on("SIGINT", () => {
    server.close();
    connection.close();
    console.log(
      `\n\x1b[31m[SERVER] \x1b[30m${localTime}\x1b[31m server off\x1b[0m`
    );
    console.log(
      `\x1b[31m[DATABASE] \x1b[30m${localTime}\x1b[31m database off\x1b[0m`
    );
  });
});