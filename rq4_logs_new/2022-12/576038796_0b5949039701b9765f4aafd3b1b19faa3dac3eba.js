import mongoose from "mongoose";

const OTP = new mongoose.Schema({
    userId: {
        type: String,
        required: true,
    },
    username: {
        type: String,
        required: true,
    },
    otpValue: {
        type: String,
        required: true,
    },
    socketId: {
        type: String,
        required: true,
    },
});

export default mongoose.model("OTP", OTP);