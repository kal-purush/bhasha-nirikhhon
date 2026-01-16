import { Account } from "src/app/model/account";
import { Counterparty } from "../../counterparty/model/counterparty";

export class DocumentSummary {
  id: number;
  type: string;
  title: string;
  documentDate: Date;
  counterparty: Counterparty;
  account: Account;
  paymentMethod: string;
}