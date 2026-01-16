export class BillRecord {
  id: string;
  userId: string;
  billId: string;
  expenseId: number;
  dateTime: string;

  constructor(options: any) {
    this.id = options.id;
    this.userId = options.userId;
    this.billId = options.billId;
    this.expenseId = options.expenseId;
    this.dateTime = options.dateTime;
  }
}