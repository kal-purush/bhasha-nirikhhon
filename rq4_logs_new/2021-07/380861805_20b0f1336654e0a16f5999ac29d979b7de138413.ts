import * as fs from "fs";
import dtsGenerator, { JsonSchema, parseSchema, ts } from "dtsgenerator";

// For usage with urls
// import nodeFetch, { Response } from "node-fetch";

// service info
const path = "swagger.json";
const nameSpace = "MagicsApi";
const serviceName = "magics";
const create = async () => {
  // const response = await nodeFetch(url).then((res: Response) => res.json());
  const response = JSON.parse(
    fs.readFileSync(path, { encoding: "utf-8" })
  ) as JsonSchema;

  const generatedContent = await dtsGenerator({
    contents: [parseSchema(response)],
    config: {
      target: ts.ScriptTarget.ES2019,
      plugins: {
        "@dtsgenerator/replace-namespace": {
          map: [
            {
              from: ["Paths"],
              to: [`${nameSpace}Paths`],
            },
            {
              from: ["Components"],
              to: [nameSpace],
            },
          ],
        },
      },
    },
  });

  fs.writeFile(
    `${serviceName}.d.ts`,
    `/* eslint-disable */\n${generatedContent}`,
    { flag: "w" },
    (err?: unknown) => {
      if (err) {
        return console.log(err);
      }
      console.log(`- types for ${serviceName} are generated.`);
    }
  );
};

// Generate
create();