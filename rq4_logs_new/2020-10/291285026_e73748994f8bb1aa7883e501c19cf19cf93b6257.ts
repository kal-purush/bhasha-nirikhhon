// Controllers must have at most 5 methods: index, show, create, update and delete
// Controllers are responsible to receive requests, forward those requests to other files and give the response back

// import { parseISO } from 'date-fns';
import { Request, Response } from 'express';
import { container } from 'tsyringe';

import ListProviderAppointmentsService from '@modules/appointments/services/ListProviderAppointmentsService';

export default class ProviderAppointmentsController {
  public async index(request: Request, response: Response): Promise<Response> {
    const provider_id = request.user.id;
    const { day, month, year } = request.body;

    // const parseDate = parseISO(date);

    const listProviderAppointmentsService = container.resolve(
      ListProviderAppointmentsService,
    );

    const appointments = await listProviderAppointmentsService.execute({
      provider_id,
      day,
      month,
      year,
    });

    return response.json(appointments);
  }
}