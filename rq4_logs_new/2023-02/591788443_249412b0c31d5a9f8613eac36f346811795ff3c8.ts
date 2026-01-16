// Main file

import * as fs from "fs";
import {
  log,
  retrieveFunction,
  retrieveFunctionLogFile,
  validateUrl,
} from "./generalFunctions";
import { repositoryClass } from "./classes";
const fetch = require("node-fetch");
require("dotenv").config();

const secretKey: any = retrieveFunction();
const [logFilePath, logLevel]: any = retrieveFunctionLogFile();

if (secretKey == undefined) {
  // What is the formal way to send here
  log("Error: Missing github token", "", logLevel);
  process.exitCode = 1;
}

if (logFilePath == undefined) {
  log("Error: Missing log file path", "", logLevel);
  process.exitCode = 1;
}
if (logLevel == undefined) {
  log("Error: Missing log file path", "", logLevel);
  process.exitCode = 1;
}

async function decomposeUrl(url: string): Promise<[string, string]> {
  return new Promise((resolve) => {
    if (
      url.startsWith("https://github.com/") ||
      url.startsWith("https://www.github.com/")
    ) {
      const parts = url.split("/");
      const org = parts[3];
      const repo = parts[4].split(".")[0];
      resolve([org, repo]);
    } else if (
      url.startsWith("https://www.npmjs.com/") ||
      url.startsWith("https://npmjs.com/")
    ) {
      const packageName = url.split("/").pop();
      const response = fetch(`https://registry.npmjs.org/${packageName}`);
      response
        .then((res) => res.json())
        .then((packageData) => {
          if (packageData.repository && packageData.repository.type == "git") {
            const repoUrl = packageData.repository.url;
            const parts = repoUrl.split("/");
            const org = parts[3];
            const repo = parts[4].split(".")[0];
            resolve([org, repo]);
          }
        });
    } else {
      log(
        `Error: ${url} is invalid. The URL must from github or npmjs`,
        "",
        logLevel
      );
    }
  });
}

export async function main(urlFile) {
  const fileContent = fs.readFileSync(urlFile, "utf-8");
  const urls = fileContent.split("\n");
  console.log(urls);
  urls.forEach(async (value, index, array) => {
    if (validateUrl(value)) {
      const [random1, random2] = await decomposeUrl(value);
      let repo = new repositoryClass(random2, value, random1);
      repo.getlicense();
      repo.getRepoInfo();
      console.log(repo.printProperties());
    } else {
      // Fix the error log here
      log(`${value} is not a valid url`, "", logLevel)
    }
  });
}