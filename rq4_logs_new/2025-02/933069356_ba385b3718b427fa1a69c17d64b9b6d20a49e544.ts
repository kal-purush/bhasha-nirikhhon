// models/Wallet.ts
import { Schema, model } from "mongoose";

const walletSchema = new Schema(
  {
    email: { type: String, required: true, unique: true },
    password: { type: String, required: true }, // Hashed password
    xion: { // Make xion required
      type: {
        address: { type: String, required: true },
        encryptedMnemonic: { type: String, required: true },
        iv: { type: String, required: true },
        salt: { type: String, required: true }
      },
      required: true
    },
    recoveryToken: { type: String, default: null },
    recoveryTokenExpiry: { type: Date, default: null }
  },
  { timestamps: true }
);

// walletSchema.index({ email: 1 }, { unique: true });

export const Wallet = model("wallets", walletSchema);