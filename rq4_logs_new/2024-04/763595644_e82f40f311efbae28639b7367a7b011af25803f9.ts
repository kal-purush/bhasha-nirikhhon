import { Payload } from '@weavcraft/common';
import _ = require('lodash');
import { Schema } from 'mongoose';

export const payloadSchema = new Schema<Payload>({
  name: { type: String, required: true },
  description: { type: String, required: false },
  createdBy: { type: Schema.ObjectId, required: true },
  updatedBy: { type: Schema.ObjectId, required: true },
  createdAt: { type: Date, default: Date.now },
  updatedAt: { type: Date, default: Date.now },
}, {
  collection: 'payloads',
  versionKey: false,
  timestamps: { createdAt: 'createdAt', updatedAt: 'updatedAt' }
});

payloadSchema.set('toJSON', {
  transform: (doc: any, ret: any) =>
    _.assign({ id: ret._id.toString() }, _.omit(ret, ['_id'])),
});

payloadSchema.set('toObject', {
  transform: (doc: any, ret: any) =>
    _.assign({ id: doc._id.toString() }, _.omit(doc, ['_id'])),
});
