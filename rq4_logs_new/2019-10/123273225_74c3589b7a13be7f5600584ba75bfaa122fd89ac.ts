#!/usr/bin/env node

import generator from "./index";

import { writeFileSync } from "fs";
import { resolve } from "path";

const outputFileName = process.argv[3] || "swagger.json";

generator({ apiKey: process.argv[2] })
  .then((swagger: object) => {
    const path = resolve(outputFileName);
    writeFileSync(path, JSON.stringify(swagger, null, "\t"), {
      encoding: "utf8",
      flag: "w"
    });

    console.log(`Created ${path}`);
  })
  .catch((err: Error) => {
    console.error(err);
    process.exit(1);
  });