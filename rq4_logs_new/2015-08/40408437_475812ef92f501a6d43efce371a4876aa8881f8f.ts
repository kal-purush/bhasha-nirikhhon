import * as express from 'express';
import routes from './routes';

export let app = express();

app.use(routes);

let server = app.listen(3100, () => {
  let host = server.address().address;
  let port = server.address().port;

  console.log(`Express app started on ${host}:${port}`);
});