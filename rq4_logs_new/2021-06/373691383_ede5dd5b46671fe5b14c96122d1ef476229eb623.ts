import { model, Schema, Document, HookNextFunction, models } from 'mongoose';
import error from '@error/ErrorDictionary';
import Aws from '@util/Aws';
import ErrorDictionary from '@error/ErrorDictionary';

export interface ApplyInterface {
  studentId: string;
  name: string;
  teamName: string;
  position: string;
  portfolio: { Bucket: string; Key: string };
}

const ApplySchema: Schema = new Schema({
  studentId: { type: String, required: true },
  name: { type: String, required: true },
  teamName: { type: String, required: true },
  position: { type: String, required: true },
  portfolio: {
    type: {
      Bucket: { type: String, required: true },
      Key: { type: String, required: true },
    },
    get: (d: ApplyInterface['portfolio']): string => {
      if (!d) throw ErrorDictionary.db.notfound();
      return Aws.S3.getSignedUrlSync({
        Bucket: d.Bucket,
        Key: d.Key,
        Expires: 300,
      });
    },
    required: true,
  },
});

export interface ApplyDocument extends Document, ApplyInterface {
  // Add Methods here
}

// ApplySchema.methods.~~

ApplySchema.pre('save', function (next: HookNextFunction): void {
  const doc = this as ApplyDocument;
  // models.Apply.findOne(
  //   {
  //     $or: [],
  //   },
  //   function (err: Error, site: ApplyDocument) {
  //     if (site) next(error.db.exists());
  //     if (err) next(err);
  //     next();
  //   },
  // );
});

const Apply = model<ApplyDocument>('Apply', ApplySchema);

export default Apply;