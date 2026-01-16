import { UserAccount } from '@/domain/entities/UserAccount'
import { IUserAccountRepository } from '@/domain/interfaces/IUserAccountRepository'
import { DynamoClient } from './client'
import { IUserAccountOnDatabase, toUserAccount } from './mappers/toUserAccount'

export class UserAccountRepository implements IUserAccountRepository {
  private TABLE_NAME = 'user_account'

  async create(userAccount: UserAccount) {
    await DynamoClient.put({
      TableName: this.TABLE_NAME,
      Item: {
        hash_key: 0,
        account_number: userAccount.accountNumber,
        cpf: userAccount.cpf,
        balance: userAccount.balance,
        created_at: userAccount.createdAt,
      },
    }).promise()

    return userAccount
  }

  async findByAccountNumber(accountNumber: number) {
    const response = await DynamoClient.query({
      TableName: this.TABLE_NAME,
      KeyConditionExpression: 'account_number = :account_number',
      ExpressionAttributeValues: {
        ':account_number': accountNumber,
      },
    }).promise()

    if (response.Items && response.Items.length > 0) {
      return toUserAccount(response.Items[0] as IUserAccountOnDatabase)
    }
    return null
  }

  async findByCpf(cpf: string) {
    const response = await DynamoClient.query({
      TableName: this.TABLE_NAME,
      IndexName: 'cpf_index',
      KeyConditionExpression: 'cpf = :cpf',
      ExpressionAttributeValues: {
        ':cpf': cpf,
      },
    }).promise()

    console.log(response)

    if (response.Items && response.Items.length > 0) {
      return toUserAccount(response.Items[0] as IUserAccountOnDatabase)
    }
    return null
  }

  async findLastAccountNumber() {
    const response = await DynamoClient.query({
      TableName: this.TABLE_NAME,
      KeyConditionExpression: 'hash_key = :hash_key',
      ExpressionAttributeValues: {
        ':hash_key': 0,
      },
      ScanIndexForward: false,
      Limit: 1,
    }).promise()

    if (!response.Items || response.Items.length === 0) {
      return 0
    } else {
      const { account_number: lastAccountNumber } = response.Items[0] as {
        account_number: number
      }
      return lastAccountNumber
    }
  }
}