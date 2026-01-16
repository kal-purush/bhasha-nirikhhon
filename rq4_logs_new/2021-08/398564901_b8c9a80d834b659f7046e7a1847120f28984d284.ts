import BaseValidator from 'App/Validator/BaseValidator'
import { APIGatewayEvent } from 'aws-lambda'
import { number } from 'joi'

export interface BaseCrudValidator {
  createValidation()
  filterValidation()
  updateByIdValidation()
  deleteByIdValidation()
}

export type BaseHttpResponse = {
  statusCode: number
  body: object
}

export default abstract class CrudController<Validator extends BaseCrudValidator, Model extends any> {
  constructor (public readonly validator: Validator, public readonly model: Model) {}

  public async create ({ body }: APIGatewayEvent): Promise<BaseHttpResponse> {
    try {
      const data = await BaseValidator.validate(body, this.validator, 'createValidation')
      console.log(data)
      return {
        statusCode: 201,
        body: data
      }
    } catch (error) {
      throw error
    }
  }

  public async deleteById ({ body }: APIGatewayEvent): Promise<BaseHttpResponse> {
    try {
      const data = await BaseValidator.validate(body, this.validator, 'deleteByIdValidation' )
      console.log('delete')
      return data
    } catch (error) {
      throw error
    }
  }

  public async load ({ body }: APIGatewayEvent): Promise<BaseHttpResponse> {
    try {
      const data = await BaseValidator.validate(body, this.validator, 'filterValidation' )
      console.log('load')
      return data
    } catch (error) {
      throw error
    }
  }

  public async updateById ({ body }: APIGatewayEvent): Promise<BaseHttpResponse> {
    try {
      const data = await BaseValidator.validate(body, this.validator, 'updateByIdValidation')
      console.log('update')
      return data
    } catch (error) {
      throw error
    }
  }
}