import { model, Schema, Document, HookNextFunction, models } from 'mongoose';
import error from '@error/ErrorDictionary';

export interface ArchiveInterface {
  year: number;
  projectName: string;
  teamMemberInfo: {
    name: string;
    github?: string;
  }[];
  projectBrief: string;
}

const ArchiveSchema: Schema = new Schema({
  year: { type: Number, required: true },
  projectName: { type: String, required: true },
  teamMemberInfo: { type: [{ name: { type: String }, github: String }] },
});

export interface ArchiveDocument extends Document, ArchiveInterface {
  // Add Methods here
}

// ArchiveSchema.methods.~~

ArchiveSchema.pre('save', function (next: HookNextFunction): void {
  const doc = this as ArchiveDocument;
  models.Archive.findOne(
    {
      $or: [],
    },
    function (err: Error, site: ArchiveDocument) {
      if (site) next(error.db.exists());
      if (err) next(err);
      next();
    },
  );
});

const Archive = model<ArchiveDocument>('Archive', ArchiveSchema);

export default Archive;