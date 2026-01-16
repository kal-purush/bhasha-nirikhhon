import pitcher = require("pitcher");
import { Module } from "./modules/app";

pitcher.forEntry(new Module()).build().appProvider.get((app, err) => {
  if (err) {
    console.error("Could not start", err);
    process.exit(1);
  }
  app.run().then(() => {
    console.log("complete!");
  }).catch((e) => {
    console.error(e);
  });
})