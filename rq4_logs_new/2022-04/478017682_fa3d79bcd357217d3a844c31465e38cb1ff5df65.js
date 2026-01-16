const express = require('express');
const app = express();
const PORT = process.env.PORT || 7000;
const path = require('path');

app.use(express.static('static'));

app.get('/', (req, res) => {
  res.sendFile(path.resolve('pages/index.html'));
});

app.get('/faqs', (req, res) => {
  const docs = [
    {
      title:'how are you',
      desc:'cool'
    }
  ]
  res.send(docs);
});

app.get('/faq', (req, res) => {

  res.status(400).send({
    message:'some thing went wrong',
    code:1258
  });
});


app.listen(PORT, () => {
  console.log(`Example app listening at http://localhost:${PORT}`);
});