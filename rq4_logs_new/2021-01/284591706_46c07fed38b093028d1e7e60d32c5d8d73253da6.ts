import { Schema, model } from 'mongoose';
import { DogSchema } from './dog';
import { ContactSchema } from './contact';

// create mongoose schema
const AddressSchema = new Schema({
  primaryAddress: { type: String, default: null },
  secondaryAddress: { type: String, default: null },
  city: { type: String, default: null },
  state: { type: String, default: null },
  zip: { type: String, default: null },
  country: { type: String, default: null },
  cars: [{ type: Schema.Types.ObjectId, default: [], ref: 'Car' }],
  dogs: [DogSchema],
  contact: { type: Schema.Types.ObjectId, default: null, ref: 'Contact' },
  contactEmbedded: { type: ContactSchema, default: null }
});

// create mongoose model
const Address = model('Address', AddressSchema);

export {
  Address,
  AddressSchema
}