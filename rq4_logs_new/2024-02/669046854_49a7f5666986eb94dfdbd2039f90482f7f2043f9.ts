import Order from "../../../model/order/order.model";
import { isValidObjectId } from "mongoose";
import ErrorHandler from "../../../error/errorHandler";
import Product from "../../../model/products/product.model";
import { IOrder } from "../../../types/order/order";

export const findOrdersByUser = async (userId: string) => {
  if (!userId || !isValidObjectId(userId)) {
    throw new ErrorHandler("Invalid User ID", 400);
  }
  return Order.find({ user: userId });
};

// Function to update product stocks
const updateProductStocks = async (orderItems: any[]) => {
  for (let item of orderItems) {
    let product = await Product.findOne({ _id: item.product.id });
    if (product) {
      // Check if the product is not null
      product.$inc("stock", -1 * item.quantity);
      await product.save();
    }
  }
};

// Function to create a new order
export const createNewOrder = async (orderData: IOrder) => {
  const order = new Order(orderData);
  await updateProductStocks(order.items);
  return await order.save();
};

// Function to delete an order
export const deleteOrderById = async (orderId: string) => {
  const order = await Order.findByIdAndDelete(orderId);
  if (!order) {
    throw new ErrorHandler("Order not found", 404);
  }
  return order;
};

// Function to update an order
export const updateOrderById = async (orderId: string, updateData: any) => {
  // Check if the order exists in the database or not
  const order = await Order.findByIdAndUpdate(orderId, updateData, {
    new: true,
  });
  if (!order) {
    throw new ErrorHandler("Order not found", 404);
  }
  return order;
};