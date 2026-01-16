import { Schema } from 'mongoose';

const TestRunSchema = new Schema({
  githubNumber: {
    type: Number,
    required: true,
  },
  results: [
    {
      id: {
        type: Number,
        required: true,
      },
      success: {
        type: Boolean,
        required: true,
      },
      runTime: Number,
      exitCode: Number,
    },
  ],
  dateOfEntry: {
    type: Date,
    default: new Date(),
  },
  lastUpdated: {
    type: Date,
    default: new Date(),
  },
});

export default TestRunSchema;