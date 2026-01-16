import { InvestmentTransaction } from "@/entities/investment_transactions.entity";
import { PlaidInfo } from "@/entities/plaid_info.entity";
import { Transaction } from "@/entities/transactions.entity";
import { Between, In } from "typeorm";

export default class TransactionRepository {

  /* TRANSACTIONS */

  public async txnFindByPlaidInfoId(plaidInfoId: number): Promise<Transaction[]> {
    const transactions: Transaction[] = await Transaction.find({ where: { plaidInfo: { id: plaidInfoId } } });
    return transactions;
  }

  public async txnFindByPlaidInfoIds(plaidInfoIds: number[]): Promise<Transaction[]> {
    const transactions: Transaction[] = await Transaction.find({ where: { plaidInfo: { id: In(plaidInfoIds) } } });
    return transactions;
  }

  public async txnFindByPlaidInfoIdsBetween(plaidInfoIds: number[], startDate: Date, endDate: Date) {
    const transactions: Transaction[] = await Transaction.find({
      where: {
        plaidInfo: { id: In(plaidInfoIds) },
        date: Between(startDate, endDate)
      }
    });
    return transactions;
  }

  /* INVESTMENT TRANSACTIONS */

  public async investTxnFindByPlaidInfoId(plaidInfoId: number): Promise<InvestmentTransaction[]> {
    const investmentTransactions: InvestmentTransaction[] = await InvestmentTransaction.find({ where: { plaidInfo: { id: plaidInfoId } } });
    return investmentTransactions;
  }

  public async investTxnFindByPlaidInfoIds(plaidInfoIds: number[]): Promise<InvestmentTransaction[]> {
    const investmentTransactions: InvestmentTransaction[] = await InvestmentTransaction.find({ where: { plaidInfo: { id: In(plaidInfoIds) } } });
    return investmentTransactions;
  }

  public async investTxnFindByPlaidInfoIdsBetween(plaidInfoIds: number[], startDate: Date, endDate: Date) {
    const investmentTransactions: InvestmentTransaction[] = await InvestmentTransaction.find({
      where: {
        plaidInfo: { id: In(plaidInfoIds) },
        date: Between(startDate, endDate)
      }
    });
    return investmentTransactions;
  }
}