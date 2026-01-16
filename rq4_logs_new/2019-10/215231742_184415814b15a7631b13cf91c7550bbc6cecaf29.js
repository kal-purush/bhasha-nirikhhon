const mongoose = require("mongoose");
const MyObjectId = mongoose.Types.ObjectId;

const userSchema = new mongoose.Schema({
  username: { type: String, required: true, index: true, unique: true },
  password: { type: String, required: true, minlength: 8 },
  trip: [MyObjectId]
});

const User = mongoose.model("User", userSchema);

module.exports = User;