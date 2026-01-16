/* 

    shopping cart

*/
const ShoppingCartModel = require("../schema/shoppingCart");
const OrderModel = require("../schema/order");
const UserModel = require("../schema/user.js");
const ProductionModel = require("../schema/production/production");
const ProductionSkuModel = require("../schema/production/production_sku");
const shortid = require('shortid');

class ShoppingCart {
    /* 
        获取购物车列表
    */
    async listShoppingCart(ctx, next) {
        const parameter = ctx.query;
        const list = await ShoppingCartModel.find({ userId: parameter.userId }).populate({ path: "skuId addressId" });
        ctx.body = {
            status: 200,
            message: "请求成功",
            data: {
                list
            }
        };
    }

    async addProduct(ctx, next) {}

    async decreaseProduct(ctx, next) {}

    /* 
        清空购物车
    */
    async cleanShoppingCart(ctx, next) {
        const parameter = ctx.request.body;
        await ShoppingCartModel.remove({ userId: parameter.userId });
        ctx.body = {
            status: 200,
            message: "清空成功"
        };
    }

    /* 
        添加到购物车
    */
    async addToShoppingCart(ctx, next) {
        const parameter = ctx.request.body;
        console.log(parameter);
        const skuResult = await ProductionSkuModel.findOne({ _id: parameter.skuId });
        console.log(skuResult);
        const productResult = await ProductionModel.findOne({ _id: parameter.productId });
        console.log(productResult);
        const userResult = await UserModel.findOne({ _id: parameter.userId }).populate("address");
        console.log(userResult);
        const addressResult = userResult.address.filter((val, index) => val.default);
        console.log(addressResult);
        const result = await ShoppingCartModel.create({
            skuId: parameter.skuId,
            userId: parameter.userId,
            addressId: addressResult.length > 0 ? addressResult[0]._id : null,
            title: productResult.title,
            number: 1,
            price: skuResult.price,
            deliveryFee: 0,
            pic: productResult.pic,
            productionId: parameter.productId,
            discount: 0,
            totalPrice: skuResult.price
        });
        ctx.body = {
            status: 200,
            message: "添加购物车成功"
        };
    }

    /* 
        提交购物车
    */
    async sumbitOrder(ctx, next) {
        const parameter = ctx.request.body;
        console.log(parameter);
        let orderId = shortid.generate();
        parameter.shoppingList.forEach(async (val, index) => {
            console.log(val);
            let result = await ShoppingCartModel.findOne({ _id: val });
            console.log(result);
            let orderResult = await OrderModel.create({
                orderId,
                skuId: result.skuId,
                addressId: result.addressId,
                title: result.title,
                number: result.number,
                price: result.price,
                deliveryFee: 0,
                totalPrice: result.totalPrice,
                expressStatus: 0,
                price: result.price,
                productionId: result.productionId,
                pic: result.pic,
                status: 1,
            });
            console.log(orderResult);
            // await ShoppingCartModel.create({})
        });
        ctx.body = {
            status: 200,
            message: '订单提交成功',
            data: {
                orderId
            }
        }
    }
    /* 

    */
}
module.exports = new ShoppingCart();