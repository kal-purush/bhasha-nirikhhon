import { IStockRepository } from '@/domain/interfaces/IStockRepository'
import { DynamoClient } from './client'
import { IStockOnDatabase, toStock } from './mappers/toStock'

export class StockRepository implements IStockRepository {
  private TABLE_NAME = 'stock'

  async findByName(name: string) {
    const response = await DynamoClient.query({
      TableName: this.TABLE_NAME,
      KeyConditionExpression: 'stock_name = :stock_name',
      ExpressionAttributeValues: {
        ':stock_name': name,
      },
    }).promise()

    if (!response.Items || response.Items.length === 0) {
      return null
    }

    return toStock(response.Items[0] as IStockOnDatabase)
  }
}