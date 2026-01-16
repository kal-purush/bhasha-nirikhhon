const express = require('express'),
      app = express();

port = process.env.PORT || 3000;

app.post('/email', (req, res) => {
  
});

app.listen(port, () => {
  console.log(`Server running on PORT ${ port }.`);
})