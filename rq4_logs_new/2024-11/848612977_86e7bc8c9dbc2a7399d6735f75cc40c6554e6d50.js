const mongoose = require("mongoose");
const Schema = mongoose.Schema;
const FieldSchema = new Schema({
  name: String,
  description: String,
  value: { type: String, default: null },
});
const ConversationalFormValueSchema = new Schema(
  {
    conversationalForm: {
      type: mongoose.Schema.Types.ObjectId,
      ref: "conversationalFormValue",
    },
    fields: [FieldSchema],
  },
  { timestamps: true }
);
const ConversationalFormValue = mongoose.model(
  "conversationalFormValue",
  ConversationalFormValueSchema,
  "conversationalFormValue"
);
module.exports = ConversationalFormValue;