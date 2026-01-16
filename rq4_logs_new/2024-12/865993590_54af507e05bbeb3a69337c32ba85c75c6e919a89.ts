import mongoose, { Schema, Document } from 'mongoose';

// Định nghĩa interface cho ArtStory
export interface IArtStory extends Document {
  title: string;
  author?: string;
  date?: Date;
  description?: string;
  content?: string;
  caption?: string[];
  imageUrl?: string[];
}


const artStorySchema: Schema = new Schema<IArtStory>({
  title: { type: String, required: true },
  author: { type: String },
  date: { type: Date, default: Date.now },
  description: { type: String },
  content: { type: String },
  caption: [{ type: String }],
  imageUrl: [{ type: String }] 
});

const ArtStoryModel = mongoose.model<IArtStory>('ArtStory', artStorySchema);
export default ArtStoryModel;