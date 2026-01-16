/* eslint no-console:0 */
/* eslint import/no-extraneous-dependencies: ["error", {"devDependencies": true}] */
import spawn from "cross-spawn";
import { delimiter, resolve as pathResolve } from "path";

process.env.REACT_APP_ENV = "development";

// Crash on unhandled rejections
process.on("unhandledRejection", (err): never => {
  throw err;
});

let argv = process.argv.slice(2);

if (argv.indexOf("--local") >= 0) {
  // Remove --local from argv
  argv = argv.filter((arg): boolean => arg !== "--local");

  // Set the env variable to use the local server
  console.log("Using Server at http://localhost:4001");
  process.env.REACT_APP_USE_LOCAL_SERVER = "true";
  process.env.PROXY = "http://localhost:4001";
} else {
  console.log("Using Server at http://prysma.local");
}

spawn.sync("react-scripts", ["start", ...argv], {
  stdio: ["ignore", "inherit", "inherit"],
  cwd: process.cwd(),
  env: Object.assign({}, process.env, {
    PATH: process.env.PATH + delimiter + pathResolve(process.cwd(), "node_modules", ".bin"),
  }),
});