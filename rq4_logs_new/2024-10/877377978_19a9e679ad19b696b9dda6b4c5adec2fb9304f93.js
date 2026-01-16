const express = require("express");
const userRouter = require("./routes/user.route");
const taskRouter = require("./routes/task.route");
const authRouter = require("./routes/auth.route");

const studentRouter = require("./routes/student.route");
const subjectRouter = require("./routes/subject.route");

const bodyParser = require("body-parser");
const app = express();

app.use(bodyParser.json());

app.use("/user", userRouter);
app.use("/task", taskRouter);
app.use("/auth", authRouter);

app.use("/student", studentRouter);
app.use("/subject", subjectRouter);

app.use("/", (req, res) => {
  res.send("Hello");
});

app.use((error, req, res, next) => {
  console.log(error.message);
  res.send({
    status: false,
    message: error.message,
  });
});

module.exports = app;