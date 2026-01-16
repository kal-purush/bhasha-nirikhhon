import mongoose from 'mongoose';
const Schema = mongoose.Schema;

const ToySchema = new Schema({
  name: { type: String, required: true, minlength: 1 },
  user: {
    type: mongoose.Schema.Types.ObjectId,
    ref: 'User',
    required: true,
  },
  image_url: { type: String, default: '' },
  manufacturer: { type: String, default: '' },
  franchise: { type: String, default: '' },
  series: { type: String, default: '' },
  description: { type: String, default: '' },
  date_posted: {
    type: Date,
    default: Date.now(),
    required: true,
  },
});

ToySchema.set('toJSON', {
  transform: (document, returnedObject) => {
    returnedObject.id = returnedObject._id.toString();
    delete returnedObject._id;
    delete returnedObject.__v;
  },
});

export default mongoose.model('Toy', ToySchema);