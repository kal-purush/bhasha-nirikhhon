import mongoose, { Schema, model } from "mongoose";

const propertiestests = new Schema(
  {
    name: {
      type: String,
    },
    summary: {
      type: String,
    },
    type: {
      type: String,
    },
    accommodates: {
      type: Number,
    },
    beds: {
      type: Number,
    },
    bedrooms: {
      type: Number,
    },
    bathrooms: {
      type: Number,
    },
    amenities: {
      type: [String],
    },
    available: {
      type: [Object],
    },
    price: {
      type: Number,
    },
    image: {
      type: String,
    },
    address: {
      type: String,
    },
    score: {
      type: Number,
    },
    id_post: {
      type: Number,
    },
  },
  { versionKey: false }
);

export const Propertiestests = model("propertiestests", propertiestests);