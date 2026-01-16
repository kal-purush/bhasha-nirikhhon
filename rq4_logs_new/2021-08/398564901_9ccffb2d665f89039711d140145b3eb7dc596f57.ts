import UserRepository from "App/Repository/UserRepository"
import User from 'App/Models/User';
import { getCustomRepository, Repository } from 'typeorm';
import EstablishmentRepository from 'App/Repository/EstablishmentRepository';
import Establishment from 'App/Models/Establishment';
import HttpException from "App/Exceptions/HttpException";

type Credentials = {
  cnpj: string
  password: string
}

export default class AuthorizerController {

  protected readonly userRepository: Repository<User> = getCustomRepository(UserRepository)
  protected readonly establishmentRepository: Repository<Establishment> = getCustomRepository(EstablishmentRepository)

  public async authorize(event, context, callback) {
    
    const { headers: { Authorization } } = event

    if (!Authorization) {
      callback('Unauthorized')
      throw new HttpException('Unauthorized', 401, event)
    }

    const token = Authorization.replace('Basic ', '')
    const [cnpj, password] = Buffer.from(token, 'base64').toString().split(':')

    const user = await this.findUser({ cnpj, password }, event, callback)

    this.allowUser(event, user, callback)
  }

  private async findUser(credentials: Credentials, event, callback) {
    const user = await this.userRepository.findOne({ where: credentials })

    if (!user) {
      throw new HttpException('User not found', 404, event)
    }

    const { id, cnpj, establishment } = user

    return {
      id, cnpj, establishment
    }
  }

  private allowUser(event, user, callback) {
    const { methodArn } = event
    const [, , , awsRegion, awsAccountId, apiGatewayArnTmp] = methodArn.split(':')
    const [restApiId, stage] = apiGatewayArnTmp.split('/')
    const apiArn = `arn:aws:execute-api:${awsRegion}:${awsAccountId}:${restApiId}/${stage}/*/*`
    const policy = {
      principalId: user,
      policyDocument: {
        Version: '2012-10-17',
        Statement: [
          {
            Action: 'execute-api:Invoke',
            Effect: 'Allow',
            Resource: [apiArn]
          }
        ]
      }
    }
    callback(null, policy)
  }

}