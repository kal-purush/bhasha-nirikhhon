const fs = require("fs-extra");

const gamesDir = "./games";

fs.ensureDirSync("./dist/games")
fs.copySync(gamesDir, "./dist/games");

console.log("Postbuild complete.");