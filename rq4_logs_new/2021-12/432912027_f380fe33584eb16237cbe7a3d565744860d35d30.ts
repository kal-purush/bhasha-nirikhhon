import {GET, Path, PathParam, QueryParam} from 'typescript-rest';
import {Inject} from 'typescript-ioc';
import {NumberApi} from '../services';
import {LoggerApi} from '../logger';

@Path('/roman-to-number')
export class NumberController {

  @Inject
  service: NumberApi;
  @Inject
  _baseLogger: LoggerApi;

  get logger() {
    return this._baseLogger.child('NumberController');
  }

  @GET
  async NumberUnknown(@QueryParam("value") value: string): Promise<number> {
    this.logger.info('Saying hello to someone');
    return this.service.number(value);
  }

}