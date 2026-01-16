// Controllers must have at most 5 methods: index (List), show, create, update and delete
// Controllers are responsible to receive requests, forward those requests to other files and give the response back

import { Request, Response } from 'express';
import { container } from 'tsyringe';

import ListProvidersService from '@modules/appointments/services/ListProvidersService';

export default class ProvidersController {
  public async index(request: Request, response: Response): Promise<Response> {
    const user_id = request.user.id;

    const listProviders = container.resolve(ListProvidersService);
    const providers = await listProviders.execute({ user_id });

    return response.json(providers);
  }
}