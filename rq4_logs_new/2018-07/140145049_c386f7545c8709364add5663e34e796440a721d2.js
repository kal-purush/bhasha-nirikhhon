const express = require('express');
const path = require('path');

const app = express();

const PORT = process.env.PORT || 5000;

const publicPath = path.join(__dirname, './public/');
app.use(express.static(publicPath));

/*
  Note: No route handlers are needed

  This is just a simple server that serves the public files, which are downloaded through index.html
*/

app.listen(PORT, () => {
  console.log(`Listening on port ${PORT}`);
});