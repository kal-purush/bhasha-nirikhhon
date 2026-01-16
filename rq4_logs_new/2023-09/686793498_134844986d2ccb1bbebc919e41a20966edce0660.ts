/*
  * Interfaces are automatically generated from the entity relationship diagram (db.vuerd.json)
*/
// Shopping Cart Item
export interface ICartItem {
    id: number;
    sessionId: number;
    productId: number;
    quantity: number;
    createdAt: string;
    updatedAt: string;
  }
  
  // Order Detaiil
  export interface IOrderDetails {
    id: number;
    userId: number;
    paymentId: number;
    addressId: number;
    total: number;
    createdAt: string;
    updatedAt: string;
  }
  
  // Order Item
  export interface IOrderItems {
    id: number;
    orderId: number;
    productId: number;
    quantity: number;
    createdAt: string;
    updatedAt: string;
  }
  
  // Payment Detail
  export interface IPaymentDetails {
    id: number;
    amount: number;
    provider: string;
    status: string;
    createdAt: string;
    updatedAt: string;
  }
  
  // Hardware Product
  export interface IProduct {
    id: number;
    categoryId: number;
    discountId: number;
    inventoryId: number;
    name: string;
    desc: string | null;
    sku: string;
    price: number;
    active: boolean;
    createdAt: string;
    updatedAt: string;
  }
  
  // Product Category
  export interface IProductCategory {
    id: number;
    name: string;
    desc: string | null;
    createdAt: string;
    modifiedAt: string;
  }
  
  // Product Discount
  export interface IProductDiscount {
    id: number;
    name: string;
    desc: string | null;
    percent: number;
    active: boolean;
    createdAt: string;
    updatedAt: string;
  }
  
  // Hardware Product Inventory
  export interface IProductInventory {
    id: number;
    quantity: number;
    createdAt: string;
    updatedAt: string;
  }
  
  // Shopping Cart
  export interface IShoppingSession {
    id: number;
    userId: number;
    total: number;
    createdAt: string;
    updatedAt: string;
  }
  
  // User
  export interface IUser {
    id: number;
    email: string;
    telephone: string;
    firstName: string;
    lastName: string;
    role: number;
    authHash: string | null;
    createdAt: string;
    modifiedAt: string;
  }
  
  // User Address
  export interface IUserAddress {
    id: number;
    userId: number;
    addressLine1: string;
    addressLine2: string;
    city: string;
    postalCode: string;
    country: string;
    telephone: string;
    mobile: string;
    createdAt: string;
    updatedAt: string;
  }
  
  // User Payment
  export interface IUserPayment {
    id: number;
    userId: number;
    paymentType: string;
    accountNo: number;
    provider: string;
    expiry: string;
  }