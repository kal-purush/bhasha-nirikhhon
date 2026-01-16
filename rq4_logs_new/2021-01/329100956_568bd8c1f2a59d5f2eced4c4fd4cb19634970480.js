var express = require('express');
const path = require('path');

// Create a new Express application.
const app = express()

app.use(require('body-parser').urlencoded({ extended: true }));

app.get('/', (req, res)=>{
 // res.sendFile(path.join(__dirname+'/test.html'));
 res.send('Hello User')
})

app.listen(3003, ()=>{
    console.log("running on port 3003")
});

// Acting As A Database 
const cyril = {
  "labels": ["1", "2", "3", "4", "5", "6", "7", "8", "9"],
  "data": [43, 27, 45, 45, 30, 40, 30, 25, 39],
  "spanGaps": false
}

// A url to get data to the server
app.get('/data1', cors() ,(req,res) => {
      res.send(cyril)
})

// A url to post data to the server
app.post('/data',(req,res) => {
  const cyril = req.body;
  console.log(cyril);
  res.end()
})