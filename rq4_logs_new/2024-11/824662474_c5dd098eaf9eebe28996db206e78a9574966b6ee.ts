import { Injectable, Inject } from '@nestjs/common';
import { ClientProxy } from '@nestjs/microservices';
import { lastValueFrom } from 'rxjs';
import { CreateCheckoutDTO } from '../../pkg/dtos/create-checkout-dto';

@Injectable()
export class ExternalCheckoutService {
  constructor(
    @Inject('CHECKOUT_MICROSERVICE') private readonly checkoutMicroserviceClient: ClientProxy,
  ) {}

  async createCheckout(createCheckoutDTO: CreateCheckoutDTO) {
    const response = this.checkoutMicroserviceClient.send('create_checkout', createCheckoutDTO);
    return await lastValueFrom(response);
  }
}