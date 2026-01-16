// models/User.ts
import { Schema, SchemaTypes, model } from "mongoose";

const userSchema = new Schema(
  {
    email: { type: String, required: true, unique: true },
    username: { type: String, required: true, unique: true },
    password: { type: String, required: true }, // Hashed
    profileImage: { type: String, default: null },
    bio: { type: String, default: null },
    isPremium: { type: Boolean, default: false },
    tel: { type: Number, default: null },
    role: {
      type: String,
      enum: ["LISTENER", "ARTIST", "ADMIN"],
      default: "LISTENER",
    },
    wallets: {
      starknet: { type: String, default: null },
      xion: {
        address: { type: String, default: null },
        encryptedMnemonic: { type: String, default: null },
        iv: { type: String, default: null },
        salt: { type: String, default: null }
      },
    },
    recoveryToken: { type: String, default: null }, // For password reset
    recoveryTokenExpiry: { type: Date, default: null },
    oauthTokens: [{ type: SchemaTypes.ObjectId, ref: "OAuthToken" }],
    artist: { type: SchemaTypes.ObjectId, ref: "artist" },
  },
  { timestamps: true }
);

export const User = model("users", userSchema);