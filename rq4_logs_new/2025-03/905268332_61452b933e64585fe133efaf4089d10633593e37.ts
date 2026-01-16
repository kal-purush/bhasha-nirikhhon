import { config } from '../config';
import { connectDB } from './database/database';
import { ApiExpress } from './api/express/AppExpress';

function main() {
  connectDB();
  const api = ApiExpress.build();
  api.start(config.apiURL, config.port);
}

main();