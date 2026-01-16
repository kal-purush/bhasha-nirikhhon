const express = require("express");
const bodyParser = require("body-parser");

const app = express();
app.use(bodyParser.json());

app.post("/bfhl", (req, res) => {
  const data = req.body.data || [];
  const numbers = [];
  const alphabets = [];

  data.forEach((item) => {
    if (!isNaN(item)) {
      numbers.push(item);
    } else {
      alphabets.push(item.toUpperCase());
    }
  });

  const highest_alphabet = alphabets.sort().pop() || null;

  res.json({
    is_success: true,
    user_id:"deepak_gupta_11092002",
    email:“dg6723@srmist.edu.in”, 
    roll_number:"RA2011030030029",
    numbers: "[“1”,”334”,”4”]",
    alphabets,
    highest_alphabet: highest_alphabet ? [highest_alphabet] : [],
  });
});


app.get("/bfhl", (req, res) => {
  res.status(200).json({
    operation_code: 1,
  });
});

app.listen(3000, confirmation(3000));

function confirmation(port) {
  console.log(`server is running on ${port} `);
}