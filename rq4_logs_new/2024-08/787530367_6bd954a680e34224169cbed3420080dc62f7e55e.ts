import { Schema, model } from 'mongoose';

const logsSchema = new Schema(
  {
    user: {
      type: Schema.Types.ObjectId,
      ref: 'User',
      required: [true, 'חובה להזין מזהה משתמש ללוג'],
    },
    action: {
      type: Schema.Types.Number,
      required: [true, 'חובה להזין מספר פעולה ללוג'],
    },
    description: {
      type: Schema.Types.String,
      required: [true, 'חובה להזין הסבר ללוג'],
    },
    sessionId: {
      type: Schema.Types.UUID,
      required: [true, 'חובה להזין מזהה סשן ללוג'],
    },
  },
  {
    timestamps: true,
    strict: false,
    toJSON: { virtuals: true },
    toObject: { virtuals: true },
  }
);

const Logs = model('Logs', logsSchema);

export default Logs;