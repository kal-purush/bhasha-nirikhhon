const functions = require('firebase-functions');

var express = require("express");
var cors = require("cors");
var mongoose = require("mongoose");
var bodyParser = require("body-parser");

var mongo_uri = "mongodb+srv://droppdeadz:coinlocker01@coinlockercluster.vqext.mongodb.net/<dbname>?retryWrites=true&w=majority";
mongoose.Promise = global.Promise;
mongoose.connect(mongo_uri, { useNewUrlParser: true }).then(
  () => {
    console.log("[success] task 2 : connected to the database ");
  },
  error => {
    console.log("[failed] task 2 " + error);
    process.exit();
  }
);

var app = express();

app.use(cors());
app.use(bodyParser.urlencoded({ extended: true }));
app.use(bodyParser.json());

app.get("/", (req, res) => {
  res.status(200).send("Hello");
});

var lockerSchema = mongoose.Schema(
  {
    name: {
      type: String
    },
    sizes: {
      type: String
    },
    status: {
      type: Boolean
    },
    startDateTime: {
      type: String
    }
  },
  {
    collection: "LOCKERS"
  }
);

var billSchema = mongoose.Schema(
  {
    name: {
      type: String
    },
    value: {
      type: Number
    },
    amount: {
      type: Number
    }
  },
  {
    collection: "BILLS"
  }
);

var coinSchema = mongoose.Schema(
  {
    name: {
      type: String
    },
    value: {
      type: Number
    },
    amount: {
      type: Number
    }
  },
  {
    collection: "COINS"
  }
);

var Locker = mongoose.model("locker", lockerSchema);
var Bill = mongoose.model("bill", billSchema);
var Coin = mongoose.model("coin", coinSchema);

app.get("/locker/", (req, res) => {
  Locker.find().exec((err, data) => {
    if (err) return res.status(400).send(err);
    res.status(200).send(data);
  });
});

app.get("/locker/:_id", (req, res) => {
  Locker.findById(req.params._id).exec((err, data) => {
    if (err) return res.status(400).send(err);
    res.status(200).send(data);
  });
});

app.put("/locker/:_id", (req, res) => {
  Locker.findByIdAndUpdate(req.params._id, req.body, (err, data) => {
    if (err) return res.status(400).send(err);
    res.status(200).send("อัพเดทข้อมูลเรียบร้อย");
  });
});

app.get("/coin/", (req, res) => {
  Coin.find().exec((err, data) => {
    if (err) return res.status(400).send(err);
    res.status(200).send(data);
  });
});

app.get("/coin/:_id", (req, res) => {
  Coin.findById(req.params._id).exec((err, data) => {
    if (err) return res.status(400).send(err);
    res.status(200).send(data);
  });
});

app.get("/bill/", (req, res) => {
  Bill.find().exec((err, data) => {
    if (err) return res.status(400).send(err);
    res.status(200).send(data);
  });
});

app.get("/bill/:_id", (req, res) => {
  Bill.findById(req.params._id).exec((err, data) => {
    if (err) return res.status(400).send(err);
    res.status(200).send(data);
  });
});


app.use((req, res, next) => {
  var err = new Error("ไม่พบ path ที่คุณต้องการ");
  err.status = 404;
  next(err);
});

exports.api = functions.https.onRequest(app);