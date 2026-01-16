import mongoose, { Schema, Document } from "mongoose";

// Interface cho sản phẩm
export interface IHideProduct extends Document {
  namePro: string; // Tên sản phẩm
  statusPro: boolean; // Trạng thái còn hàng (true = còn hàng, false = hết hàng)
  price: number; // Giá sản phẩm
  quantity: number; // Số lượng sản phẩm
  cateId: mongoose.Types.ObjectId; // Danh mục sản phẩm
  imgPro: string[]; // Hình ảnh của sản phẩm
  isHide: boolean; // Trạng thái ẩn (true = ẩn, false = hiển thị)
}

// Định nghĩa Schema cho sản phẩm
const HideProductSchema = new mongoose.Schema<IHideProduct>({
  namePro: { type: String, required: true }, // Tên sản phẩm
  statusPro: { type: Boolean, required: true, default: true }, // Trạng thái còn hàng (true = còn hàng, false = hết hàng)
  price: { type: Number, required: true }, // Giá sản phẩm
  quantity: { type: Number, required: true }, // Số lượng sản phẩm
  cateId: { type: mongoose.Schema.Types.ObjectId, ref: "Category", required: true }, // Danh mục sản phẩm
  imgPro: { type: [String], required: true }, // Hình ảnh của sản phẩm
  isHide: { type: Boolean, required: true, default: false }, // Trạng thái ẩn (true = ẩn, false = hiển thị)
});

// Export model từ schema
export default mongoose.model<IHideProduct>("HideProduct", HideProductSchema);