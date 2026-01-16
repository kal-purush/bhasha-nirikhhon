const express = require("express");
const app = express();
const cors = require("cors");
const mongoose = require("mongoose");
const bodyParser = require("body-parser");
require("dotenv").config();

app.use(cors());
app.use(express.static("public"));
app.use(bodyParser.json());
app.use(bodyParser.urlencoded({ extended: true }));
app.get("/", (req, res) => {
  res.sendFile(__dirname + "/views/index.html");
});

const listener = app.listen(process.env.PORT || 3000, () => {
  console.log("Your app is listening on port " + listener.address().port);
});

mongoose.connect(process.env["MONGO_URI"], {
  useNewUrlParser: true,
  useUnifiedTopology: true,
});

const userSchema = new mongoose.Schema({
  username: {
    type: String,
    required: true,
  },
});

const User = mongoose.model("User", userSchema);

app.post("/api/users", (req, res) => {
  const name = req.body["username"];

  if (!name) {
    throw "invalid username";
  }

  const user = new User({
    username: name,
  });

  user
    .save()
    .then((data) => {
      res.json({ username: name, _id: data.id });
    })
    .catch((err) => {
      throw err;
    });
});

app.get("/api/users", (req, res) => {
  User.find({})
    .then((data) => {
      var userMap = [];
      data.forEach((element) => {
        var user = {};
        user["_id"] = element._id;
        user["username"] = element.username;
        userMap.push(user);
      });
      res.send(userMap);
    })
    .catch((err) => {
      throw err;
    });
});

const exerciseSchema = mongoose.Schema({
  userid: String,
  description: String,
  duration: Number,
  date: Date,
});

const Exercise = mongoose.model("Exercise", exerciseSchema);

app.post("/api/users/:_id/exercises", (req, res) => {
  const _id = req.params["_id"];

  // check if user exists with given id
  User.findById(_id)
    .then((data) => {
      let date;
      try {
        if (req.body["date"]) {
          const temp = req.body["date"].split("-");
          date = new Date(temp);
        } else date = new Date().now();
      } catch (err) {
        throw err;
      }

      const exer = new Exercise({
        userid: _id,
        date: date,
        duration: req.body["duration"],
        description: req.body["description"],
      });

      exer
        .save()
        .then((dat) => {
          res.json({
            _id: data._id,
            username: data.username,
            date: new Date(dat.date).toDateString(),
            duration: dat.duration,
            description: dat.description,
          });
        })
        .catch((err) => {
          throw err;
        });
    })
    .catch((err) => {
      throw err;
    });
});

app.get("/api/users/:_id/logs", (req, res) => {
  const _id = req.params["_id"];
  const from = req.query["from"];
  const to = req.query["to"];
  const limit = parseInt(req.query["limit"], 10) || 0;
  User.findById(_id)
    .then((user) => {
      let query = { userid: user._id };

      if (from || to) {
        query.date = {};
        if (from) {
          query.date.$gte = new Date(from);
        }
        if (to) {
          query.date.$lte = new Date(to);
        }
      }

      Exercise.find(query)
        .limit(limit)
        .then((exercises) => {
          const log = exercises.map((e) => ({
            date: e.date.toDateString(),
            duration: e.duration,
            description: e.description,
          }));

          res.json({
            _id: user._id,
            username: user.username,
            count: log.length,
            log: log,
          });
        })
        .catch((err) => {
          throw err;
        });
    })
    .catch((err) => {
      throw err;
    });
});