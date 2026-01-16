const express = require('express')
const bodyParser = require("body-parser");
const app = express()


app.use(bodyParser.urlencoded({extended: true}));

app.set('view engine', 'ejs');

app.get('/', (req, res) => {

    var today = new Date();

    var numberDay = today.getDay();
 
    const daysOfWeek = ["Pazartesi", "Salı", "Çarşamba", "perşembe", "Cuma", "Cumartesi", "Pazar"]

    var day = daysOfWeek[numberDay];

    res.render("index", {goDay: day});
      
  
});

app.listen(3000, () => {
  console.log(`Example app listening on port 3000`)
})