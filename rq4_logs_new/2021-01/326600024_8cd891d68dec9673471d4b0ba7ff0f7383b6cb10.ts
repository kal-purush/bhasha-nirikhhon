import { Schema, model } from 'mongoose';
import { nanoid } from 'nanoid';
import mongooseLeanVirtuals from 'mongoose-lean-virtuals';
import { MemberDocument } from '../../../types/index';

const schema = new Schema(
  {
    _id: {
      type: String,
      default() {
        return nanoid();
      },
    },
    username: {
      type: String,
      required: true,
    },
    password: {
      type: String,
      required: true,
    },
    realName: String,
  },
  { timestamps: true },
);

schema.plugin(mongooseLeanVirtuals);

const Member = model<MemberDocument>('Member', schema);

export { Member };