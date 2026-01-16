export class EnrolmentEdit {
    filter(arg0: (student: any) => boolean): any {
        throw new Error("Method not implemented.");
    }
  constructor(public id: string, public amount_paid: string, public created_at: string,
    public expiry_date: string, public currency_code: string, public house_id: string,
    public mastercode: string, public payment_status: string,
    public progress: string, public purchaser_id: string, public role_id: string,
    public start_date: string, public transaction_id: string,
    public updated_at: string,
    public user_id: string, public user_name: string,
    public places_alloted: string,
  ) {
  }
} 