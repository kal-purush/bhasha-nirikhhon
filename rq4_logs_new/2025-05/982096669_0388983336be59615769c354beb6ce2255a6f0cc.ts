import { Schema, model, Document } from "mongoose";

export interface ICategory extends Document {
  name: string;
  slug: string;
  description?: string;
  created_at: Date;
  updated_at: Date;
}

const CategorySchema = new Schema<ICategory>(
  {
    name: { type: String, required: true },
    slug: { type: String, required: true, unique: true },
    description: { type: String },
  },
  { timestamps: { createdAt: "created_at", updatedAt: "updated_at" } }
);

// Add indexes for better query performance
CategorySchema.index({ name: 1 });
CategorySchema.index({ slug: 1 }, { unique: true });

// Export model, checking if it exists first to prevent overwrite errors
const CategoryModel = model<ICategory>("Category", CategorySchema);

export { CategoryModel };