import { Schema, model } from 'mongoose';

interface IPost {
  id: string;
  title: string;
  content: string;
  likes: number;
  author: string;
  geolocation: {
    lat: number;
    lng: number;
  }
}

const PostSchema = new Schema<IPost>({
  id: { type: String, required: true },
  title: { type: String, required: true },
  content: { type: String, required: true },
  likes: { type: Number, default: 0 },
  author: { type: String, required: true },
  geolocation: Object,
});

const Post = model<IPost>('Post', PostSchema);

export default Post;