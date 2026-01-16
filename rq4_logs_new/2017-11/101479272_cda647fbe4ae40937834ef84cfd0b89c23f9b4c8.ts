// create Cognito User
// add to Cognito User Group

import { FunctionHandler, Handler, HTTP, Log, Logger } from '@ananseio/serverless-common';
import * as Joi from 'joi';
import { IoT, Congito, CustomerDB } from '../../../lib';
import { CreateUser, CreateUserError } from '../../../public';

export class CreateUserHandler extends FunctionHandler {
  public static handler: Function;
  public log: Logger;

  private iot = new IoT();
  private congito = new Congito();
  private customerDB = new CustomerDB();

  @Handler
  @Log(HTTP)
  @HTTP({ cors: { credentials: true } })
  public async handler(event: CreateUser.Request): Promise<HTTP.Response<CreateUser.Response>> {
    try {
      this.log.debug(this.rawEvent);
      const isValid = Joi.validate(event, CreateUserError, { allowUnknown: true, abortEarly: false });
      if (isValid.error) {
        return this.resp.badRequest({ error: isValid.error.message });
      }

      const customer = await this.customerDB.getCustomer(event.body.customerName);
      if (!customer) {
        return this.resp.badRequest({ error: CreateUserError.customerError });
      }
      if (customer.token !== event.body.token) {
        return this.resp.forbidden({ error: CreateUserError.authenticationError });
      }
      const username = `${customer.shortCode}-${event.body.username}`;
      const oneTimePassword = await this.congito.createUser(username, event.body.email);
      await this.congito.adduserToGroup(username, customer.name);

      return this.resp.ok({ status: 'success' });
    } catch (err) {
      this.log.error(err);
      return this.resp.internalError({ error: CreateUserError.generalError });
    }
  }
}