const express = require("express");
const connectDB = require("./db/connect");
require("dotenv").config();
app = express();
//app.use(json());

const PORT = process.env.PORT;
const start = async () => {
    await connectDB(process.env.DB_URL)
    app.listen(PORT, console.log('server is listening to port ' + PORT))
}
start();