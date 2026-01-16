const mongoose = require("mongoose");

const EventSchema = new mongoose.Schema({
  name: String,
  location: String,
  day: Number,
  month: String,
  year: Number,
  price: Number,
  regUsers: [
    {
      type: mongoose.SchemaTypes.ObjectId,
      ref: "UserSchema",
    },
  ],
});

const model = mongoose.model("EventSchema", EventSchema);

module.exports = model;