export class RoomPaymentForPrint {
  roomNumber: number;
  tenants: Array<string>;
  dueDates: Array<number>;
  monthMinusTwo?: {
    oneMonthAdvance: Array<number>;
    electricBill: string;
    waterBill: string;
    riceCookerBill: string;
  };
  monthMinusOne?: {
    oneMonthAdvance: Array<number>;
    electricBill: string;
    waterBill: string;
    riceCookerBill: string;
  };
  currentMonth?: {
    oneMonthAdvance: Array<number>;
    electricBill: string;
    waterBill: string;
    riceCookerBill: string;
  };
}