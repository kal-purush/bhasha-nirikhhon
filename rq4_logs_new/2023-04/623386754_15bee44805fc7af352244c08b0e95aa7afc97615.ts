import { Schema, model, Types } from "mongoose";
// INTERFACE
import { TokenInterface } from "../utilities/interfaces";
const TokenSchema = new Schema<TokenInterface>(
  {
    refreshToken: { type: String, required: true },
    ip: { type: String, required: true },
    userAgent: { type: String, required: true },
    isValid: { type: Boolean, default: true },
    user: { type: Types.ObjectId, ref: "User", required: true },
  },
  { timestamps: true }
);

const Token = model("Token", TokenSchema);

export default Token;