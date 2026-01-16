import mongoose from "mongoose";

const ChatSchema = new mongoose.Schema({
  senderId: { type: String, maxlength: 255, required: true },
  receiverId: { type: String, maxlength: 255, required: true },
  message: { type: String, required: true },
  chatType: { type: String, enum: ['Văn bản', 'Hình ảnh', 'Video'], required: true },
  timestamp: { type: Date, default: Date.now },
  chatStatus: { type: String, enum: ['Đã gửi', 'Đã nhận', 'Đã đọc'], default: 'Đã gửi' },
});

export default mongoose.model("Chat", ChatSchema);