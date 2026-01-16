import * as express from 'express'

const app = express();

const port = process.env.SLACKHUNT_API_SERVER_PORT || 3001

app.listen(port, () => console.log(`api server listening on ${port}`))