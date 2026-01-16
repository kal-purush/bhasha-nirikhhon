//TODO: fix the seed database, because there is something wrong with data retrieval (response body is always empty)
import fs from 'fs';
import util from 'util';
import path from 'path';
import mongoose from 'mongoose';
const readDir = util.promisify(fs.readdir);

function toTitleCase(str: string) {
  return str.replace(/\w\S*/g, (txt: string) => {
    return txt.charAt(0).toUpperCase() + txt.substr(1).toLowerCase();
  });
}

// Load seeds of all models
async function seedDatabase(runSaveMiddleware = false) {
  const dir = await readDir(__dirname);
  const seedFiles = dir.filter((file) => file.endsWith('.seed.js'));

  for (const file of seedFiles) {
    const fileName = file.split('.seed.js')[0];
    const modelName = toTitleCase(fileName);
    const model = mongoose.models[modelName];

    if (!model) throw new Error(`Cannot find Model '${modelName}'`);
    const fileContents = require(path.join(__dirname, file));

    runSaveMiddleware
      ? await model.create(fileContents)
      : await model.insertMany(fileContents);
  }
}

export const seedDb = seedDatabase;