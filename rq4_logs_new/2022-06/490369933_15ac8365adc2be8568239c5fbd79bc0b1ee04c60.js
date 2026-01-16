const Orders = require('../models/order')
const OrderItems = require('../models/orderitem')
const { ORDER_STATUSES } = require('../helpers/index')
exports.validateComment = async (req, res, next) => {
    try {
        const userId = req.user._id
        const { product } = req.body
        const receivedOrders = await Orders.find({
            userId: userId,
            status: ORDER_STATUSES.DELIVERED,
        })
            .select('_id')
            .lean()
        if (!receivedOrders.length) {
            var err = new Error('Bạn chỉ thể đánh giá sản phẩm mình đã mua!');
            err.status = 500;
            throw err;
        }
        const orderItems = await OrderItems.find({
            product, order: { $in: receivedOrders.map((order) => order._id) },
        })
            .select('_id')
            .lean()
        if (!orderItems.length) {
            var err = new Error('Bạn chỉ thể đánh giá sản phẩm mình đã mua!');
            err.status = 500;
            throw err;
        }
        next(err)
    } catch (error) {
        next(error);
    }
}