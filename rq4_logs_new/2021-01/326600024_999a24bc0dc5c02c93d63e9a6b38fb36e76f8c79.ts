import { Schema, model } from 'mongoose';
import { nanoid } from 'nanoid';
import mongooseLeanVirtuals from 'mongoose-lean-virtuals';
import {
  PromoEnrollmentDocument,
  PromoEnrollmentStatus,
} from '../../../types/index';

const schema = new Schema(
  {
    _id: {
      type: String,
      default() {
        return nanoid();
      },
    },
    promo: {
      type: String,
      required: true,
      ref: 'Promo',
    },
    status: {
      type: String,
      enum: [
        PromoEnrollmentStatus.Approved,
        PromoEnrollmentStatus.Pending,
        PromoEnrollmentStatus.Processing,
        PromoEnrollmentStatus.Rejected,
      ],
      required: true,
      default: PromoEnrollmentStatus.Pending,
    },
    member: {
      type: String,
      required: true,
      ref: 'Member',
    },
  },
  { timestamps: true },
);

schema.plugin(mongooseLeanVirtuals);

const PromoEnrollment = model<PromoEnrollmentDocument>(
  'PromoEnrollment',
  schema,
);

export { PromoEnrollment };