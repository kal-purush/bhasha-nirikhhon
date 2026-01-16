import _ = require('lodash');
import { Schema } from 'mongoose';
import { Hierarchy, EnumHierarchyType } from '@weavcraft/common';

export const hierarchySchema = new Schema<Hierarchy>({
  title: { type: String, required: true },
  type: { 
    type: String,
    enum: EnumHierarchyType,
    required: true,
  },
  category: { type: String, required: true },
  description: { type: String, required: false },
  superior: { type: String, required: false },
  payload: {
    type: Object,
    default: false,
    required: false,
  },
  createdAt: { type: Date, default: Date.now },
  updatedAt: { type: Date, default: Date.now },
}, {
  versionKey: false,
  timestamps: { createdAt: 'createdAt', updatedAt: 'updatedAt' }
});

hierarchySchema.set('toJSON', {
  transform: (doc: any, ret: any) =>
    _.assign({ id: ret._id.toString() }, _.omit(ret, ['_id'])),
});

hierarchySchema.set('toObject', {
  transform: (doc: any, ret: any) =>
    _.assign({ id: doc._id.toString() }, _.omit(doc, ['_id'])),
});