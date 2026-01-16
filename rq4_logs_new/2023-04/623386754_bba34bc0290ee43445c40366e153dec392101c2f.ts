import { ObjectId, Document } from "mongoose";
import CurrencyInterface from "../payment/CurrencyInterface";

interface SingleOrderSchemaInterface extends Document {
  amount: number;
  price: number;
  product: ObjectId | string;
}
interface SingleOrderModelInterface extends SingleOrderSchemaInterface {
  updateProductStock({
    productId,
    amount,
    operation,
  }: {
    productId: string;
    amount: number;
    operation: "+" | "-";
  }): Promise<void>;
}

interface OrderSchemaInterface extends CurrencyInterface, Document {
  orderItems: object[];
  totalPrice: number;
  status: "pending" | "failed" | "paid" | "delivered" | "canceled";
  user: ObjectId | string;
  clientSecret: string;
  paymentIntentID: string;
}

export {
  SingleOrderSchemaInterface,
  OrderSchemaInterface,
  SingleOrderModelInterface,
};