import { UserEntity } from "../../user/UserEntity";
import { CartItemEntity } from "../../shopping-session/cart-item/CartItemEntity";
import { OrderDetails } from "../../../entities/OrderDetails";
import { OrderItems } from "../../../entities/OrderItems";
import { PaymentDetails } from "../../../entities/PaymentDetails";
import { Product } from "../../../entities/Product";
import { ProductCategory } from "../../../entities/ProductCategory";
import { ProductDiscount } from "../../../entities/ProductDiscount";
import { ProductInventory } from "../../../entities/ProductInventory";
import { ShoppingSessionEntity } from "../../shopping-session/ShoppingSessionEntity";
import { UserAddress } from "../../../entities/UserAddress";
import { UserPayment } from "../../../entities/UserPayment";

export const entities = [
  UserEntity,
  CartItemEntity,
  OrderDetails,
  OrderItems,
  PaymentDetails,
  Product,
  ProductCategory,
  ProductDiscount,
  ProductInventory,
  ShoppingSessionEntity,
  UserAddress,
  UserPayment,
];

export default entities;