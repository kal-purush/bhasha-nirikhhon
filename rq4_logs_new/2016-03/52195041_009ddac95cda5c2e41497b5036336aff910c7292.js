import mongoose from 'mongoose';
import shortid from 'shortid';

const Schema = mongoose.Schema;

const ListSchema = new Schema({
  _id: { type: String, default: shortid.generate },
  name: { type: String, required: true },
  created_at: { type: Date, default: Date.now },
  updated_at: { type: Date },
});

ListSchema.pre('save', function presave(next) {
  this.updated_at = Date.now();
  return next();
});

export default mongoose.model('List', ListSchema);