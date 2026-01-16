/*import express from 'express';
import cors from 'cors';

const app = express();
const port = 3001;

app.use(cors());

let visitorCount = 0;

app.get('/visitors', (req, res) => {
  visitorCount += 1;
  res.json({ count: visitorCount });
});

app.listen(port, () => {
  console.log(`Visitor counter API listening at http://localhost:${port}`);
});*/
import express from 'express';
import cors from 'cors';

const app = express();
app.use(cors());

let visitorCount = 0;

app.get('/visitors', (req, res) => {
  visitorCount += 1;
  res.json({ count: visitorCount });
});

const port = 3001;
app.listen(port, () => {
  console.log(`Visitor counter API listening at http://localhost:${port}/visitors`);
});
