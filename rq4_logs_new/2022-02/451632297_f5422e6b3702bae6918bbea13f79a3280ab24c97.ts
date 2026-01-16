import { writeFile } from 'fs';
import * as dotenv from 'dotenv';

// Configure Angular `environment.ts` file path
const targetPath = './src/environments/environment.ts';
// Load node modules
// require('dotenv').load();
dotenv.config();
// `environment.ts` file structure
const envConfigFile = `export const environment = {
  production: false,
  firebaseConfig: {
    apiKey: '${process.env['API_KEY'] || ""}',
    authDomain: '${process.env['AUTH_DOMAIN'] || ""}',
    projectId: '${process.env['PROJECT_ID'] || ""}',
    storageBucket: '${process.env['STORAGE_BUCKET'] || ""}',
    messagingSenderId: '${process.env['MESSAGE_IN_SENDER_ID'] || ""}',
    appId: '${process.env['APP_ID'] || ""}',
    measurementId: '${process.env['APIMEASUREMENT_ID_KEY'] || ""}'
  }
};
`;

console.log('The file `environment.ts` will be written with the following content');
writeFile(targetPath, envConfigFile, function (err) {
   if (err) {
       throw console.error(err);
   } else {
       console.log(`Angular environment.ts file generated correctly at ${targetPath} \n`);
   }
});