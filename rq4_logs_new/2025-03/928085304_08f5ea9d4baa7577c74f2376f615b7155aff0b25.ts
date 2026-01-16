export interface Receipt {
  storeName: string;
  businessNum: string;
  owner: string;
  phoneNumber: string;
  storePlace: string;
  orderId: number;
  receiptDate: string;
  placeName: string;
  joinNumber: string;
  totalAmount: number;
  createdAt: string;
  menuList: {
    orderMenuId: number;
    menuId: number;
    menuName: string;
    discountRate: number;
    totalPrice: number;
    totalCount: number;
  }[];
  cardInfoList: {
    paymentType: "CARD" | "CASH";
    cardCompany: string;
    cardNumber: string;
    inputMethod: string;
    approveDate: string;
    approveNumber: string;
    installmentPeriod: string;
    paidMoney: number;
  }[];
}