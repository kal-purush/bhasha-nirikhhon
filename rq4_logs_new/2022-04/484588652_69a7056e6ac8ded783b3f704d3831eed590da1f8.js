const express = require("express");
const mongoose = require("mongoose");
const app = express();
const cors = require('cors')

app.use(cors())
app.use(express.json()) ;


//Import Routes
const authRoute = require('./routes/auth');


//Route middleWares
app.use('/user', authRoute)


//Connect to database
mongoose.connect("mongodb+srv://greg:npm766m5@notescluster.9regt.mongodb.net/notes?retryWrites=true&w=majority", {
  useNewUrlParser: true, 
}, () => console.log("connected to databse") );













app.listen(3001, () => {
  console.log(`Server listening on 3001`);
});