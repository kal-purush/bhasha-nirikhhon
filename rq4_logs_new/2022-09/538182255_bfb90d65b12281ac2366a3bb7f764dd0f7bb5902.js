const { Schema, Types } = require("mongoose");

// This will not be a model, but rather will be used as the reaction field's subdocument schema in the Thought model.

const reactionSchema = new Schema({
  reactionId: {
    type: Schema.Types.ObjectId,
    default: () => new Types.ObjectId(),
  },
  reactionBody: {
    type: String,
    required: true,
    // 280 characters maximum
    maxlength: 280,
  },
  username: {
    type: String,
    required: true,
  },
  createdAt: {
    type: Date,
    // Sets the default value to the current timestamp
    default: Date.now,
    // Getter method to format the timestamp on query
    get: (date) => {
      if (date) {
        return date.toLocaleDateString();
      }
    },
  },
});

module.exports = reactionSchema;