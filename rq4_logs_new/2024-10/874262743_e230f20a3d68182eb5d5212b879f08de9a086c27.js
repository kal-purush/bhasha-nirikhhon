const express = require("express");
const app = express();
const port = 5500;
const cors = require('cors')
app.use(cors());
// db connection
const dbConnection = require("./db/dbConfig");

//user routes middleware file
const userRoutes = require("./routes/userRoute");
//question routes middleware file
const questionsRoute = require("./routes/questionRoute");
//answer routes middleware file
const answerRoute=require("./routes/answerRoute")

//authentication middleware
const authMiddleware = require("./middleware/authMiddleware");
app.use(express.json());

//user routes mddleware
app.use("/api/user", userRoutes);

//questions routes middleware ??
app.use("/api/questions", authMiddleware, questionsRoute);

// answers routes middleware??
app.use("/api/answer", authMiddleware, answerRoute);

async function start() {
  try {
    const result = await dbConnection.execute("SELECT 'Test'");
    // console.log(result);
    await app.listen(port);
    console.log("database connection established");
    console.log(`listening on ${port}`);
  } catch (error) {
    console.log(error.message);
  }
}
start();