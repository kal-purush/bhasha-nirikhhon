// EXPRESS
import { RequestHandler } from "express";
// INTERFACES
import {
  CurrencyInterface,
  CartItemsInterface,
  SingleOrderSchemaInterface,
  SingleOrderQuery,
} from "../utilities/interfaces";
// MODELS
import { Product, Order, SingleOrder } from "../models";
// UTILITIES
import {
  findDocumentByIdAndModel,
  gteAndLteQueryForDb,
  limitAndSkip,
  userIdAndModelUserIdMatchCheck,
} from "../utilities/controllers";
// PAYMENT
import createPayment from "../utilities/payment/payment";
// ERRORS
import { BadRequestError, PaymentRequiredError } from "../errors";
// HTTP STATUS CODES
import { StatusCodes } from "http-status-codes";

const createOrder: RequestHandler = async (req, res) => {
  // CLIENT SIDE REQUESTS
  const {
    cartItems,
    currency,
  }: {
    cartItems: CartItemsInterface;
    currency: CurrencyInterface;
  } = req.body;
  // THROW AN ERROR IF THERE IS NO CART ITEMS OR CURRENCY
  if (!cartItems || currency) throw new BadRequestError("invalid credientals");
  // ALL ORDERS IN AN ARRAY TO APPEND IT TO ORDER MODEL LATER
  let orderItems: SingleOrderSchemaInterface[] = [];
  // TOTAL PRICE OF SINGLE ORDERS
  let subTotal: number = 0;
  // *LOOP THROUGH CLIENT CART ITEMS OBJECT
  for (let i = 0; i < cartItems.length; i++) {
    // SINGLE CART ITEM BY CLIENT
    const { amount, price, tax, product: productId } = cartItems[i];
    // FIND THE DOCUMENT OF PRODUCT
    const product = await findDocumentByIdAndModel({
      id: productId,
      MyModel: Product,
    });
    if (price !== product.price)
      throw new BadRequestError("price does not match");
    // CREATE A SINGLE ORDER
    const singleOrder = await SingleOrder.create({ amount, price, product });
    // APPEND THIS ORDER TO ORDERITEMS ARRAY
    orderItems = [...orderItems, singleOrder];
    // PRODUCT ORDER PRICE AS GBP
    const productOrderPrice = amount * price;
    // TAX VALUE WITHOUT DOT
    const taxValueWithoutDot = Number((tax / 100).toString().replace(".", ""));
    // APPEND TAX RATE TO EVERY ITEM DEPENDS ON THEIR TAX VALUE
    const productOrderPriceWithTax = productOrderPrice * taxValueWithoutDot;
    // APPEND PRODUCT ORDER PRICE TO SUBTOTAL
    subTotal += productOrderPriceWithTax;
  }
  // *LOOP END
  // SHIPPING FEE AS GBP. IF TOTAL SHOPPING IS ABOVE 75 GBP THEN FREE
  const shippingFee = subTotal >= 75 ? 0 : 999;
  // APPEND SHIPPING FEE TO FIND TOTAL PRICE
  const totalPrice = subTotal + shippingFee;
  // CREATE THE PAYMENT INTENT
  const paymentIntent = await createPayment({ totalPrice, currency });
  // CHECK IF PAYMENT INTENT EXISTS
  if (!paymentIntent) throw new PaymentRequiredError("payment required");
  // IF PAYMENT INTENT PROPERTIES DO NOT EXIST THAN THROW AN ERROR
  const { amount, client_secret, id: paymentIntentId } = paymentIntent;
  if (!amount || client_secret || paymentIntentId)
    throw new PaymentRequiredError("payment required");
  // CREATE ACTUAL ORDER HERE
  const order = await Order.create({
    orderItems,
    shippingFee,
    subTotal,
    totalPrice,
    user: req.user?._id,
    clientSecret: client_secret,
    paymentIntentID: paymentIntentId,
  });
  // SENT CLIENT SECRET TO THE CLIENT
  res.status(StatusCodes.CREATED).json({ msg: "order created", client_secret });
};

// *ONLY FOR ADMIN
const getAllSingleOrders: RequestHandler = async (req, res) => {
  // GET CLIENT SIDE QUERIES
  const {
    amount,
    price,
    tax,
    product: productId,
    orderPage,
  }: SingleOrderSchemaInterface & { orderPage: number } = req.body;
  // EMPTY QUERY
  const query: Partial<SingleOrderQuery> = {};
  // SET QUERY KEYS AND VALUES
  if (amount) query.amount = amount;
  if (price) query.price = gteAndLteQueryForDb(price.toString());
  if (tax) query.tax = tax;
  if (productId) query.product = productId;
  // FIND DOCUMENTS OF SINGLE ORDERS
  const singleOrder = SingleOrder.find(query);
  // LIMIT AND SKIP VALUES
  const myLimit = 20;
  const { limit, skip } = limitAndSkip({ limit: myLimit, page: orderPage });
  const result = await singleOrder.skip(skip).limit(limit);
  // RESPONSE
  res.status(StatusCodes.OK).json({ msg: "single orders fetched", result });
};

// *ONLY FOR ADMIN
const getSingleOrder: RequestHandler = async (req, res) => {
  // GET CLIENT SIDE QUERIES
  const { product: productId }: { product: string } = req.body;
  // IF PRODUCT ID DOES NOT EXIST THROW AN ERROR
  if (!productId) throw new BadRequestError("product id is required");
  // FIND THE SINGLE ORDER
  const singleOrder = await findDocumentByIdAndModel({
    id: productId,
    MyModel: SingleOrder,
  });
  // RESPONSE
  res
    .status(StatusCodes.OK)
    .json({ msg: "single order fetched", result: singleOrder });
};