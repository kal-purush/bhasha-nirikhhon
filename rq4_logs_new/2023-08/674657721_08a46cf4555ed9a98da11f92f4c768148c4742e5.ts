import { InvestmentTransaction } from '@/domain/entities/InvestmentTransaction'
import { TransferTransaction } from '@/domain/entities/TransferTransaction'
import { ITransactionsRepository } from '@/domain/interfaces/ITransactionsRepository'
import { DynamoClient } from './client'

export class TransactionsRepository implements ITransactionsRepository {
  private TRANSACTION_TABLE_NAME = 'transaction'
  private USER_ACCOUNT_TABLE_NAME = 'user_account'

  async createTransfer(transferTransaction: TransferTransaction) {
    const { accountNumber, amount, createdAt, event, origin } =
      transferTransaction

    const result = await DynamoClient.transactWrite({
      TransactItems: [
        {
          Put: {
            TableName: this.TRANSACTION_TABLE_NAME,
            Item: {
              account_number: accountNumber,
              created_at: createdAt,
              event,
              origin,
              amount,
            },
          },
        },
        {
          Update: {
            TableName: this.USER_ACCOUNT_TABLE_NAME,
            Key: {
              hash_key: 0,
              account_number: accountNumber,
            },
            UpdateExpression: 'ADD balance :amount',
            ExpressionAttributeValues: {
              ':amount': amount,
            },
          },
        },
      ],
    }).promise()

    return transferTransaction
  }

  async createInvestment(investmentTransaction: InvestmentTransaction) {
    const { accountNumber, createdAt, event, price, quantity, stock } =
      investmentTransaction

    const amount = -1 * price * quantity

    const result = await DynamoClient.transactWrite({
      TransactItems: [
        {
          Put: {
            TableName: this.TRANSACTION_TABLE_NAME,
            Item: {
              account_number: accountNumber,
              created_at: createdAt,
              event,
              quantity,
              price,
              stock,
            },
          },
        },
        {
          Update: {
            TableName: this.USER_ACCOUNT_TABLE_NAME,
            Key: {
              hash_key: 0,
              account_number: accountNumber,
            },
            UpdateExpression: 'ADD balance :amount',
            ExpressionAttributeValues: {
              ':amount': amount,
            },
          },
        },
      ],
    }).promise()

    return investmentTransaction
  }
}