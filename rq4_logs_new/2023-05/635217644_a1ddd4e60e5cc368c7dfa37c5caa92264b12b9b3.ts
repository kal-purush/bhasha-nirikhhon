import { Container } from 'inversify'
import 'reflect-metadata'
import AuthService from '@/contracts/interfaces/AuthService'
import { Auth } from '@/services/api/Auth'
import { HttpRequestService } from '@/contracts/interfaces/HttpRequestService'
import { AxiosService } from '@/services/Axios/AxiosService'

const container: Container = new Container()

container.bind<AuthService>('AuthService').to(Auth)
container.bind<HttpRequestService>('HttpRequestService').to(AxiosService)

export default container