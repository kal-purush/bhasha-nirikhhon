// AUTH
import RegisterVerificationInterface from "./auth/RegisterVerificationInterface";
import ResetPasswordInterface from "./auth/ResetPasswordInterface";
// PRODUCT
import GetAllProductsQueryInterface from "./product/GetAllProductsQueryInterface";
import GetAllProductsReqBodyInterface from "./product/GetAllProductsReqBodyInterface";
// ORDER
import {
  OrderClientReqInterface,
  SingleOrderQuery,
} from "./order/orderQueryInterfaces";
// CUSTOM
import priceQueryInterface from "./custom/priceQueryInterface";

export {
  RegisterVerificationInterface,
  ResetPasswordInterface,
  GetAllProductsQueryInterface,
  GetAllProductsReqBodyInterface,
  OrderClientReqInterface,
  priceQueryInterface,
  SingleOrderQuery,
};