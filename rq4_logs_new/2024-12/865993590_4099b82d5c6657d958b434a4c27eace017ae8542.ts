import mongoose from 'mongoose'; // Chỉ import mongoose một lần

const FeebackSchema = new mongoose.Schema({
    cusId: {
        type: mongoose.Schema.Types.ObjectId, // Dùng ObjectId để liên kết đến khách hàng
        ref: 'customer', // Tham chiếu đến collection 'customer'
        required: true
    },
    prodId: {
        type: mongoose.Schema.Types.ObjectId, // Dùng ObjectId để liên kết đến sản phẩm
        ref: 'product', // Tham chiếu đến collection 'product'
        required: true
    },
    stars: { // Đổi tên từ 'start' thành 'stars' cho đúng nghĩa
        type: Number,
        required: true,
        min: 1,
        max: 5 // Xếp hạng thường từ 1 đến 5
    },
    content: {
        type: String,
        maxlength: 255
    },
    dateFeed: {
        type: Date,
        default: Date.now // Đặt ngày mặc định là ngày hiện tại
    }
});

const FeebackModel = mongoose.model('feedback', FeebackSchema);
export default FeebackModel; // Sử dụng export thay vì module.exports nếu dùng ES6