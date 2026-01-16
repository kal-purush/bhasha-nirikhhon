const path = require('path');
const express = require('express');
const app = express();

const PORT = 8080;

app.use('/', express.static(path.join(__dirname, 'public')));

app.listen(PORT, () => {
  console.log(`Snake game listening on port '${PORT}'`);
});